from __future__ import annotations

import os
import re
from contextlib import contextmanager
from typing import Any, Iterator

from ..models import (
    ColumnInfo,
    ColumnProfile,
    ForeignKeyInfo,
    IndexInfo,
    InspectionResult,
    PrimaryKeyInfo,
    SnapshotOptions,
    TableInfo,
    TableProfile,
    ViewInfo,
)
from ..safety import validate_readonly_sql

TABLES_SQL = """
SELECT
    current_database() AS database_name,
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'p' THEN 'partitioned_table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        ELSE c.relkind::text
    END AS kind,
    CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint ELSE NULL END AS row_estimate,
    CASE WHEN c.relkind IN ('r', 'p', 'm') THEN pg_total_relation_size(c.oid) ELSE NULL END AS size_bytes,
    obj_description(c.oid, 'pg_class') AS comment
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
  AND c.relkind IN ('r', 'p', 'v', 'm')
ORDER BY n.nspname, c.relname
"""

COLUMNS_SQL = """
SELECT
    current_database() AS database_name,
    c.table_schema,
    c.table_name,
    c.column_name,
    c.ordinal_position,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    pg_catalog.col_description(format('%I.%I', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position) AS comment
FROM information_schema.columns c
WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.table_schema, c.table_name, c.ordinal_position
"""

PRIMARY_KEYS_SQL = """
SELECT
    current_database() AS database_name,
    kcu.table_schema,
    kcu.table_name,
    kcu.column_name,
    kcu.ordinal_position,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
 AND tc.constraint_schema = kcu.constraint_schema
 AND tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kcu.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position
"""

FOREIGN_KEYS_SQL = """
SELECT
    current_database() AS database_name,
    kcu.table_schema,
    kcu.table_name,
    kcu.column_name,
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
 AND tc.constraint_schema = kcu.constraint_schema
 AND tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_catalog = tc.constraint_catalog
 AND ccu.constraint_schema = tc.constraint_schema
 AND ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND kcu.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY kcu.table_schema, kcu.table_name, tc.constraint_name, kcu.ordinal_position
"""

INDEXES_SQL = """
SELECT
    current_database() AS database_name,
    schemaname AS schema_name,
    tablename AS table_name,
    indexname AS index_name,
    indexdef
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename, indexname
"""

VIEWS_SQL = """
SELECT
    current_database() AS database_name,
    table_schema,
    table_name,
    view_definition
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name
"""

NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "real",
    "double precision",
    "smallserial",
    "serial",
    "bigserial",
}
TIME_TYPES = {"date", "timestamp", "timestamp without time zone", "timestamp with time zone", "time with time zone", "time without time zone"}
TEXT_TYPES = {"text", "character varying", "character", "varchar", "char", "uuid", "json", "jsonb"}


class MissingPostgresDriverError(RuntimeError):
    pass


class MissingDSNError(RuntimeError):
    pass


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def parse_timeout(value: str | float | int) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = value.strip().lower()
    if text.endswith("ms"):
        return float(text[:-2]) / 1000
    if text.endswith("s"):
        return float(text[:-1])
    if text.endswith("m"):
        return float(text[:-1]) * 60
    return float(text)


def parse_index_columns(indexdef: str | None) -> list[str]:
    if not indexdef:
        return []
    match = re.search(r"\((.*)\)", indexdef)
    if not match:
        return []
    raw = match.group(1)
    columns: list[str] = []
    current: list[str] = []
    depth = 0
    for char in raw:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            columns.append("".join(current).strip().strip('"'))
            current = []
        else:
            current.append(char)
    if current:
        columns.append("".join(current).strip().strip('"'))
    return columns


class PostgresConnector:
    def __init__(self, dsn: str, timeout_seconds: float = 5.0) -> None:
        self.dsn = dsn
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls, dsn_env: str, timeout_seconds: float = 5.0) -> "PostgresConnector":
        dsn = os.environ.get(dsn_env)
        if not dsn:
            raise MissingDSNError(f"environment variable {dsn_env} is not set")
        return cls(dsn=dsn, timeout_seconds=timeout_seconds)

    @contextmanager
    def connect(self) -> Iterator[Any]:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ModuleNotFoundError as exc:
            raise MissingPostgresDriverError(
                "psycopg is required for live Postgres connections; install contextty with runtime dependencies"
            ) from exc

        timeout_ms = max(1, int(self.timeout_seconds * 1000))
        options = f"-c default_transaction_read_only=on -c statement_timeout={timeout_ms}"
        conn = psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row, options=options)
        try:
            with conn.cursor() as cur:
                cur.execute("SET default_transaction_read_only = on")
                cur.execute("SELECT set_config('statement_timeout', %s, false)", (str(timeout_ms),))
                cur.execute("SELECT set_config('idle_in_transaction_session_timeout', %s, false)", (str(timeout_ms),))
            yield conn
        finally:
            conn.close()


class PostgresIntrospector:
    tables_sql = TABLES_SQL
    columns_sql = COLUMNS_SQL
    primary_keys_sql = PRIMARY_KEYS_SQL
    foreign_keys_sql = FOREIGN_KEYS_SQL
    indexes_sql = INDEXES_SQL
    views_sql = VIEWS_SQL

    def inspect(self, conn: Any) -> InspectionResult:
        table_rows = self._fetchall(conn, TABLES_SQL)
        column_rows = self._fetchall(conn, COLUMNS_SQL)
        pk_rows = self._fetchall(conn, PRIMARY_KEYS_SQL)
        fk_rows = self._fetchall(conn, FOREIGN_KEYS_SQL)
        index_rows = self._fetchall(conn, INDEXES_SQL)
        view_rows = self._fetchall(conn, VIEWS_SQL)

        database = table_rows[0]["database_name"] if table_rows else (
            column_rows[0]["database_name"] if column_rows else "postgres"
        )

        tables = [
            TableInfo(
                database=row["database_name"],
                schema=row["schema_name"],
                name=row["table_name"],
                kind=row["kind"],
                row_estimate=row["row_estimate"],
                size_bytes=row["size_bytes"],
                comment=row["comment"],
            )
            for row in table_rows
        ]
        columns = [
            ColumnInfo(
                database=row["database_name"],
                schema=row["table_schema"],
                table=row["table_name"],
                name=row["column_name"],
                ordinal=row["ordinal_position"],
                data_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                default=row["column_default"],
                character_max_length=row["character_maximum_length"],
                numeric_precision=row["numeric_precision"],
                numeric_scale=row["numeric_scale"],
                comment=row["comment"],
            )
            for row in column_rows
        ]
        primary_keys = [
            PrimaryKeyInfo(
                database=row["database_name"],
                schema=row["table_schema"],
                table=row["table_name"],
                column=row["column_name"],
                ordinal=row["ordinal_position"],
                constraint_name=row["constraint_name"],
            )
            for row in pk_rows
        ]
        foreign_keys = [
            ForeignKeyInfo(
                database=row["database_name"],
                schema=row["table_schema"],
                table=row["table_name"],
                column=row["column_name"],
                ref_schema=row["foreign_table_schema"],
                ref_table=row["foreign_table_name"],
                ref_column=row["foreign_column_name"],
                constraint_name=row["constraint_name"],
            )
            for row in fk_rows
        ]
        indexes = [
            IndexInfo(
                database=row["database_name"],
                schema=row["schema_name"],
                table=row["table_name"],
                name=row["index_name"],
                columns=parse_index_columns(row["indexdef"]),
                unique=" UNIQUE " in f" {row['indexdef'].upper()} ",
                primary=" PRIMARY KEY " in f" {row['indexdef'].upper()} ",
                definition=row["indexdef"],
            )
            for row in index_rows
        ]
        views = [
            ViewInfo(
                database=row["database_name"],
                schema=row["table_schema"],
                name=row["table_name"],
                definition=row["view_definition"],
            )
            for row in view_rows
        ]
        return InspectionResult(
            database=database,
            tables=tables,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=indexes,
            views=views,
        )

    def profile(
        self,
        conn: Any,
        inspection: InspectionResult,
        options: SnapshotOptions,
    ) -> dict[tuple[str, str], TableProfile]:
        profiles: dict[tuple[str, str], TableProfile] = {}
        columns_by_table: dict[tuple[str, str], list[ColumnInfo]] = {}
        for column in inspection.columns:
            columns_by_table.setdefault((column.schema, column.table), []).append(column)

        for table in inspection.tables:
            if table.kind not in {"table", "partitioned_table", "materialized_view"}:
                profiles[(table.schema, table.name)] = TableProfile(row_count=table.row_estimate)
                continue

            if options.profile_mode == "basic":
                profiles[(table.schema, table.name)] = TableProfile(row_count=table.row_estimate)
                continue

            table_profile = self._profile_table(conn, table, columns_by_table.get((table.schema, table.name), []), options)
            profiles[(table.schema, table.name)] = table_profile

        return profiles

    def execute_readonly_query(self, conn: Any, sql: str) -> list[dict[str, Any]]:
        validate_readonly_sql(sql)
        return self._fetchall(conn, sql)

    def _profile_table(
        self,
        conn: Any,
        table: TableInfo,
        columns: list[ColumnInfo],
        options: SnapshotOptions,
    ) -> TableProfile:
        table_name = qualified_table(table.schema, table.name)
        count_sql = f"SELECT count(*) AS row_count FROM (SELECT 1 FROM {table_name} LIMIT %s) AS contextty_sample"
        row = self._fetchone(conn, count_sql, (options.row_limit,))
        row_count = int(row["row_count"]) if row else 0
        profile = TableProfile(
            sample_count=row_count,
            row_count=row_count,
            row_count_is_capped=row_count >= options.row_limit,
        )

        for column in columns:
            try:
                profile.columns[column.name] = self._profile_column(conn, table, column, options)
            except Exception as exc:  # pragma: no cover - defensive around heterogeneous databases.
                profile.columns[column.name] = ColumnProfile(
                    top_values=[{"value": "__contextty_profile_error__", "count": 1, "error": str(exc)}]
                )

        timestamp_columns = [column for column in columns if column.data_type.lower() in TIME_TYPES]
        if timestamp_columns:
            profile.time_windows = self._profile_time_windows(conn, table, timestamp_columns[0], options)
        return profile

    def _profile_column(
        self,
        conn: Any,
        table: TableInfo,
        column: ColumnInfo,
        options: SnapshotOptions,
    ) -> ColumnProfile:
        table_name = qualified_table(table.schema, table.name)
        column_name = quote_ident(column.name)
        sql = f"""
        SELECT
            count(*) AS sample_count,
            count(*) FILTER (WHERE {column_name} IS NULL) AS null_count,
            count(DISTINCT {column_name}) AS distinct_count
        FROM (
            SELECT {column_name}
            FROM {table_name}
            LIMIT %s
        ) AS contextty_sample
        """
        row = self._fetchone(conn, sql, (options.row_limit,))
        sample_count = int(row["sample_count"] or 0)
        null_count = int(row["null_count"] or 0)
        profile = ColumnProfile(
            null_count=null_count,
            null_rate=(null_count / sample_count) if sample_count else None,
            distinct_count=int(row["distinct_count"] or 0),
        )

        data_type = column.data_type.lower()
        if data_type in NUMERIC_TYPES or data_type in TIME_TYPES:
            minmax_sql = f"""
            SELECT min({column_name}) AS min_value, max({column_name}) AS max_value
            FROM (
                SELECT {column_name}
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
                LIMIT %s
            ) AS contextty_sample
            """
            minmax = self._fetchone(conn, minmax_sql, (options.row_limit,))
            if minmax:
                profile.min_value = minmax["min_value"]
                profile.max_value = minmax["max_value"]

        if data_type in TEXT_TYPES or profile.distinct_count <= 50:
            top_sql = f"""
            SELECT {column_name} AS value, count(*) AS count
            FROM (
                SELECT {column_name}
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
                LIMIT %s
            ) AS contextty_sample
            GROUP BY {column_name}
            ORDER BY count(*) DESC, {column_name}
            LIMIT 10
            """
            profile.top_values = [
                {"value": row["value"], "count": int(row["count"])}
                for row in self._fetchall(conn, top_sql, (options.row_limit,))
            ]

        if data_type in TEXT_TYPES:
            sample_sql = f"""
            SELECT {column_name} AS value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT %s
            """
            values = [str(row["value"]) for row in self._fetchall(conn, sample_sql, (min(options.row_limit, 1000),))]
            profile.patterns = text_patterns(values)

        return profile

    def _profile_time_windows(
        self,
        conn: Any,
        table: TableInfo,
        column: ColumnInfo,
        options: SnapshotOptions,
    ) -> list[dict[str, Any]]:
        table_name = qualified_table(table.schema, table.name)
        column_name = quote_ident(column.name)
        window = options.time_window if options.time_window in {"hour", "day", "week", "month"} else "day"
        sql = f"""
        SELECT date_trunc(%s, {column_name}) AS window_start, count(*) AS count
        FROM (
            SELECT {column_name}
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT %s
        ) AS contextty_sample
        GROUP BY window_start
        ORDER BY window_start
        LIMIT 50
        """
        return [
            {"window_start": row["window_start"], "count": int(row["count"])}
            for row in self._fetchall(conn, sql, (window, options.row_limit))
        ]

    @staticmethod
    def _fetchall(conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            rows = cur.fetchall()
            if not rows:
                return []
            if isinstance(rows[0], dict):
                return list(rows)
            names = [column[0] for column in cur.description]
            return [dict(zip(names, row)) for row in rows]

    @classmethod
    def _fetchone(cls, conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
        rows = cls._fetchall(conn, sql, params)
        return rows[0] if rows else None


def text_patterns(values: list[str], limit: int = 10) -> list[dict[str, Any]]:
    buckets: dict[str, int] = {}
    for value in values:
        tokens = _pattern_tokens(value)
        template = " ".join(tokens)
        buckets[template] = buckets.get(template, 0) + 1
    return [
        {"template": template, "count": count}
        for template, count in sorted(buckets.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def _pattern_tokens(value: str) -> list[str]:
    words = re.findall(r"[A-Za-z]+|\d+|[0-9a-fA-F-]{8,}|[^\w\s]", value[:500])
    tokens: list[str] = []
    for word in words:
        lowered = word.lower()
        if re.fullmatch(r"\d+", lowered):
            tokens.append("<num>")
        elif re.fullmatch(r"[0-9a-f]{8,}(?:-[0-9a-f]{4,})*", lowered):
            tokens.append("<id>")
        elif len(lowered) > 32:
            tokens.append("<text>")
        else:
            tokens.append(lowered)
    return tokens or ["<empty>"]
