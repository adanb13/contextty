from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .common import text_patterns
from ..facts import MAX_SOURCE_ROWS_PER_TABLE, build_row_facts, sample_columns_for_table
from ..models import (
    ColumnInfo,
    ColumnProfile,
    ForeignKeyInfo,
    IndexInfo,
    InspectionResult,
    PrimaryKeyInfo,
    SnapshotOptions,
    SnapshotRun,
    Source,
    TableInfo,
    TableProfile,
    ViewInfo,
)
from ..safety import validate_readonly_sql

TABLES_SQL = """
SELECT
    current_database() AS database_name,
    t.table_schema AS schema_name,
    t.table_name,
    CASE t.table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        ELSE lower(replace(t.table_type, ' ', '_'))
    END AS kind,
    NULL::BIGINT AS row_estimate,
    NULL::BIGINT AS size_bytes,
    NULL::VARCHAR AS comment
FROM information_schema.tables t
WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
  AND t.table_schema NOT LIKE 'pg_%'
  AND t.table_type IN ('BASE TABLE', 'VIEW')
ORDER BY t.table_schema, t.table_name
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
    NULL::VARCHAR AS comment
FROM information_schema.columns c
WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
  AND c.table_schema NOT LIKE 'pg_%'
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
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kcu.table_schema NOT IN ('information_schema', 'pg_catalog')
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
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
JOIN information_schema.referential_constraints rc
  ON rc.constraint_catalog = tc.constraint_catalog
 AND rc.constraint_schema = tc.constraint_schema
 AND rc.constraint_name = tc.constraint_name
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_catalog = rc.unique_constraint_catalog
 AND ccu.constraint_schema = rc.unique_constraint_schema
 AND ccu.constraint_name = rc.unique_constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND kcu.table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY kcu.table_schema, kcu.table_name, tc.constraint_name, kcu.ordinal_position
"""

INDEXES_SQL = """
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    expressions,
    sql,
    is_unique
FROM duckdb_indexes()
WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
  AND schema_name NOT LIKE 'pg_%'
ORDER BY schema_name, table_name, index_name
"""

VIEWS_SQL = """
SELECT
    current_database() AS database_name,
    v.table_schema,
    v.table_name,
    v.view_definition
FROM information_schema.views v
WHERE v.table_schema NOT IN ('information_schema', 'pg_catalog')
  AND v.table_schema NOT LIKE 'pg_%'
ORDER BY v.table_schema, v.table_name
"""

NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "hugeint",
    "utinyint",
    "usmallint",
    "uinteger",
    "ubigint",
    "float",
    "double",
    "decimal",
    "numeric",
    "real",
    "boolean",
}
TIME_TYPES = {"date", "time", "timestamp", "timestamp with time zone", "timestamp_ns", "timestamp_s", "timestamp_ms"}
TEXT_TYPES = {"varchar", "char", "bpchar", "text", "uuid", "json"}


class MissingDuckDBDriverError(RuntimeError):
    pass


class DuckDBConnector:
    def __init__(self, path: str | Path, timeout_seconds: float = 5.0) -> None:
        self.path = Path(path).expanduser()
        self.timeout_seconds = timeout_seconds

    @contextmanager
    def connect(self) -> Iterator[Any]:
        try:
            import duckdb
        except ModuleNotFoundError as exc:
            raise MissingDuckDBDriverError(
                "duckdb is required for live DuckDB connections; install contextty with runtime dependencies"
            ) from exc

        conn = duckdb.connect(str(self.path.resolve()), read_only=True)
        try:
            yield conn
        finally:
            conn.close()


class DuckDBIntrospector:
    tables_sql = TABLES_SQL
    columns_sql = COLUMNS_SQL
    primary_keys_sql = PRIMARY_KEYS_SQL
    foreign_keys_sql = FOREIGN_KEYS_SQL
    indexes_sql = INDEXES_SQL
    views_sql = VIEWS_SQL

    def __init__(self, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds

    def inspect(self, conn: Any) -> InspectionResult:
        table_rows = self._fetchall(conn, TABLES_SQL)
        column_rows = self._fetchall(conn, COLUMNS_SQL)
        pk_rows = self._fetchall(conn, PRIMARY_KEYS_SQL)
        fk_rows = self._fetchall_best_effort(conn, FOREIGN_KEYS_SQL)
        index_rows = self._fetchall_best_effort(conn, INDEXES_SQL)
        view_rows = self._fetchall(conn, VIEWS_SQL)

        database = _first_database_name(table_rows, column_rows, self._database_name(conn))
        tables = [
            TableInfo(
                database=row["database_name"] or database,
                schema=row["schema_name"],
                name=row["table_name"],
                kind=row["kind"],
                row_estimate=_int_or_none(row["row_estimate"]),
                size_bytes=_int_or_none(row["size_bytes"]),
                comment=row["comment"],
            )
            for row in table_rows
        ]
        columns = [
            ColumnInfo(
                database=row["database_name"] or database,
                schema=row["table_schema"],
                table=row["table_name"],
                name=row["column_name"],
                ordinal=int(row["ordinal_position"]),
                data_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                default=row["column_default"],
                character_max_length=_int_or_none(row["character_maximum_length"]),
                numeric_precision=_int_or_none(row["numeric_precision"]),
                numeric_scale=_int_or_none(row["numeric_scale"]),
                comment=row["comment"],
            )
            for row in column_rows
        ]
        primary_keys = [
            PrimaryKeyInfo(
                database=row["database_name"] or database,
                schema=row["table_schema"],
                table=row["table_name"],
                column=row["column_name"],
                ordinal=int(row["ordinal_position"]),
                constraint_name=row["constraint_name"],
            )
            for row in pk_rows
        ]
        foreign_keys = [
            ForeignKeyInfo(
                database=row["database_name"] or database,
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
                database=row["database_name"] or database,
                schema=row["schema_name"],
                table=row["table_name"],
                name=row["index_name"],
                columns=duckdb_index_columns(row["expressions"]),
                unique=bool(row["is_unique"]),
                primary=False,
                definition=row["sql"],
            )
            for row in index_rows
        ]
        views = [
            ViewInfo(
                database=row["database_name"] or database,
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
            if table.kind != "table":
                profiles[(table.schema, table.name)] = TableProfile(row_count=table.row_estimate)
                continue
            if options.profile_mode == "basic":
                profiles[(table.schema, table.name)] = TableProfile(row_count=table.row_estimate)
                continue
            profiles[(table.schema, table.name)] = self._profile_table(
                conn,
                table,
                columns_by_table.get((table.schema, table.name), []),
                options,
            )
        return profiles

    def execute_readonly_query(self, conn: Any, sql: str) -> list[dict[str, Any]]:
        validate_readonly_sql(sql)
        return self._fetchall(conn, sql)

    def derive_facts(
        self,
        conn: Any,
        source: Source,
        run: SnapshotRun,
        inspection: InspectionResult,
        options: SnapshotOptions,
    ) -> list[dict[str, Any]]:
        if options.profile_mode != "deep":
            return []
        rows_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
        limit = max(0, min(options.row_limit, MAX_SOURCE_ROWS_PER_TABLE))
        if limit <= 0:
            return []
        for table in inspection.tables:
            if table.kind != "table":
                continue
            columns = sample_columns_for_table(inspection, table)
            if not columns:
                continue
            rows_by_table[(table.schema, table.name)] = self._sample_rows(conn, table, columns, limit)
        return build_row_facts(source, run, inspection, rows_by_table, options)

    def _profile_table(
        self,
        conn: Any,
        table: TableInfo,
        columns: list[ColumnInfo],
        options: SnapshotOptions,
    ) -> TableProfile:
        table_name = qualified_table(table.schema, table.name)
        row = self._fetchone(
            conn,
            f"SELECT count(*) AS row_count FROM (SELECT 1 FROM {table_name} LIMIT ?) AS contextty_sample",
            (options.row_limit,),
        )
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

        timestamp_columns = [column for column in columns if duckdb_type_kind(column.data_type) == "time"]
        if timestamp_columns:
            profile.time_windows = self._profile_time_windows(conn, table, timestamp_columns[0], options)
        return profile

    def _sample_rows(
        self,
        conn: Any,
        table: TableInfo,
        columns: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        table_name = qualified_table(table.schema, table.name)
        column_sql = ", ".join(quote_ident(column) for column in columns)
        return self._fetchall(conn, f"SELECT {column_sql} FROM {table_name} LIMIT ?", (limit,))

    def _profile_column(
        self,
        conn: Any,
        table: TableInfo,
        column: ColumnInfo,
        options: SnapshotOptions,
    ) -> ColumnProfile:
        table_name = qualified_table(table.schema, table.name)
        column_name = quote_ident(column.name)
        row = self._fetchone(
            conn,
            f"""
            SELECT
                count(*) AS sample_count,
                count(*) FILTER (WHERE {column_name} IS NULL) AS null_count,
                count(DISTINCT {column_name}) AS distinct_count
            FROM (
                SELECT {column_name}
                FROM {table_name}
                LIMIT ?
            ) AS contextty_sample
            """,
            (options.row_limit,),
        )
        sample_count = int(row["sample_count"] or 0)
        null_count = int(row["null_count"] or 0)
        distinct_count = int(row["distinct_count"] or 0)
        profile = ColumnProfile(
            null_count=null_count,
            null_rate=(null_count / sample_count) if sample_count else None,
            distinct_count=distinct_count,
        )

        data_kind = duckdb_type_kind(column.data_type)
        if data_kind in {"numeric", "time"}:
            minmax = self._fetchone(
                conn,
                f"""
                SELECT min({column_name}) AS min_value, max({column_name}) AS max_value
                FROM (
                    SELECT {column_name}
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    LIMIT ?
                ) AS contextty_sample
                """,
                (options.row_limit,),
            )
            if minmax:
                profile.min_value = minmax["min_value"]
                profile.max_value = minmax["max_value"]

        if data_kind == "text" or distinct_count <= 50:
            profile.top_values = [
                {"value": row["value"], "count": int(row["count"])}
                for row in self._fetchall(
                    conn,
                    f"""
                    SELECT {column_name} AS value, count(*) AS count
                    FROM (
                        SELECT {column_name}
                        FROM {table_name}
                        WHERE {column_name} IS NOT NULL
                        LIMIT ?
                    ) AS contextty_sample
                    GROUP BY {column_name}
                    ORDER BY count(*) DESC, {column_name}
                    LIMIT 10
                    """,
                    (options.row_limit,),
                )
            ]

        if data_kind == "text":
            values = [
                str(row["value"])
                for row in self._fetchall(
                    conn,
                    f"""
                    SELECT {column_name} AS value
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    LIMIT ?
                    """,
                    (min(options.row_limit, 1000),),
                )
            ]
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
        return [
            {"window_start": row["window_start"], "count": int(row["count"])}
            for row in self._fetchall(
                conn,
                f"""
                SELECT CAST(date_trunc(?, {column_name}) AS VARCHAR) AS window_start, count(*) AS count
                FROM (
                    SELECT {column_name}
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    LIMIT ?
                ) AS contextty_sample
                GROUP BY window_start
                ORDER BY window_start
                LIMIT 50
                """,
                (window, options.row_limit),
            )
            if row["window_start"] is not None
        ]

    def _database_name(self, conn: Any) -> str:
        row = self._fetchone(conn, "SELECT current_database() AS database_name")
        return row["database_name"] if row and row["database_name"] else "duckdb"

    def _fetchall_best_effort(
        self,
        conn: Any,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        try:
            return self._fetchall(conn, sql, params)
        except Exception:
            return []

    def _fetchall(
        self,
        conn: Any,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        with self._bounded(conn):
            cursor = conn.execute(sql, params or ())
            rows = cursor.fetchall()
            if not rows:
                return []
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row)) for row in rows]

    def _fetchone(self, conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
        rows = self._fetchall(conn, sql, params)
        return rows[0] if rows else None

    @contextmanager
    def _bounded(self, conn: Any) -> Iterator[None]:
        expired = False

        def interrupt() -> None:
            nonlocal expired
            expired = True
            if hasattr(conn, "interrupt"):
                conn.interrupt()

        timer = threading.Timer(max(0.001, self.timeout_seconds), interrupt)
        timer.daemon = True
        timer.start()
        try:
            yield
        except Exception as exc:
            if expired:
                raise TimeoutError(f"DuckDB query exceeded {self.timeout_seconds:g}s timeout") from exc
            raise
        finally:
            timer.cancel()


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def duckdb_type_kind(data_type: str) -> str:
    normalized = data_type.lower()
    if normalized in NUMERIC_TYPES or normalized.startswith("decimal"):
        return "numeric"
    if normalized in TIME_TYPES or "timestamp" in normalized:
        return "time"
    if normalized in TEXT_TYPES or "varchar" in normalized or "text" in normalized:
        return "text"
    return "other"


def duckdb_index_columns(expressions: Any) -> list[str]:
    if expressions is None:
        return []
    if isinstance(expressions, (list, tuple)):
        return [str(expression).strip('"') for expression in expressions]
    text = str(expressions).strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    return [part.strip().strip('"') for part in text.split(",") if part.strip()]


def _first_database_name(first: list[dict[str, Any]], second: list[dict[str, Any]], default: str) -> str:
    for rows in (first, second):
        if rows and rows[0].get("database_name"):
            return rows[0]["database_name"]
    return default


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None
