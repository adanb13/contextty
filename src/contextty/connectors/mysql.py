from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator
from urllib.parse import parse_qs, unquote, urlparse

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
    DATABASE() AS database_name,
    t.table_schema AS schema_name,
    t.table_name,
    CASE t.table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        ELSE LOWER(REPLACE(t.table_type, ' ', '_'))
    END AS kind,
    t.table_rows AS row_estimate,
    t.data_length + t.index_length AS size_bytes,
    NULLIF(t.table_comment, '') AS comment
FROM information_schema.tables t
WHERE t.table_schema = DATABASE()
  AND t.table_type IN ('BASE TABLE', 'VIEW')
ORDER BY t.table_schema, t.table_name
"""

COLUMNS_SQL = """
SELECT
    DATABASE() AS database_name,
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
    NULLIF(c.column_comment, '') AS comment
FROM information_schema.columns c
WHERE c.table_schema = DATABASE()
ORDER BY c.table_schema, c.table_name, c.ordinal_position
"""

PRIMARY_KEYS_SQL = """
SELECT
    DATABASE() AS database_name,
    kcu.table_schema,
    kcu.table_name,
    kcu.column_name,
    kcu.ordinal_position,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_schema = kcu.constraint_schema
 AND tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kcu.table_schema = DATABASE()
ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position
"""

FOREIGN_KEYS_SQL = """
SELECT
    DATABASE() AS database_name,
    kcu.table_schema,
    kcu.table_name,
    kcu.column_name,
    kcu.referenced_table_schema AS foreign_table_schema,
    kcu.referenced_table_name AS foreign_table_name,
    kcu.referenced_column_name AS foreign_column_name,
    kcu.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_schema = kcu.constraint_schema
 AND tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND kcu.table_schema = DATABASE()
  AND kcu.referenced_table_name IS NOT NULL
ORDER BY kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position
"""

INDEXES_SQL = """
SELECT
    DATABASE() AS database_name,
    s.table_schema AS schema_name,
    s.table_name,
    s.index_name,
    s.non_unique,
    s.seq_in_index,
    s.column_name,
    s.index_type
FROM information_schema.statistics s
WHERE s.table_schema = DATABASE()
ORDER BY s.table_schema, s.table_name, s.index_name, s.seq_in_index
"""

VIEWS_SQL = """
SELECT
    DATABASE() AS database_name,
    v.table_schema,
    v.table_name,
    v.view_definition
FROM information_schema.views v
WHERE v.table_schema = DATABASE()
ORDER BY v.table_schema, v.table_name
"""

MYSQL_SCHEMES = {"mysql", "mysql+pymysql"}

NUMERIC_TYPES = {
    "bit",
    "bool",
    "boolean",
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
    "decimal",
    "dec",
    "numeric",
    "float",
    "double",
    "real",
    "year",
}
TIME_TYPES = {"date", "datetime", "timestamp", "time"}
TEXT_TYPES = {
    "char",
    "varchar",
    "tinytext",
    "text",
    "mediumtext",
    "longtext",
    "enum",
    "set",
    "json",
}


class MissingMySQLDriverError(RuntimeError):
    pass


class MissingDSNError(RuntimeError):
    pass


class UnsupportedDSNError(ValueError):
    pass


def quote_ident(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def parse_mysql_dsn(
    dsn: str,
    allowed_schemes: set[str] | frozenset[str] = MYSQL_SCHEMES,
    default_port: int = 3306,
) -> dict[str, Any]:
    parsed = urlparse(dsn)
    if parsed.scheme not in allowed_schemes:
        raise UnsupportedDSNError(f"unsupported DSN scheme for MySQL-compatible source: {parsed.scheme}")

    query = parse_qs(parsed.query)
    database = unquote(parsed.path.lstrip("/"))
    if not database:
        raise UnsupportedDSNError("MySQL-compatible DSNs must include a database name")

    connect_args: dict[str, Any] = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or default_port,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
        "charset": _query_value(query, "charset") or "utf8mb4",
    }
    unix_socket = _query_value(query, "unix_socket")
    if unix_socket:
        connect_args["unix_socket"] = unix_socket
    return connect_args


class MySQLConnector:
    schemes = MYSQL_SCHEMES

    def __init__(self, dsn: str, timeout_seconds: float = 5.0) -> None:
        self.dsn = dsn
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls, dsn_env: str, timeout_seconds: float = 5.0) -> "MySQLConnector":
        dsn = os.environ.get(dsn_env)
        if not dsn:
            raise MissingDSNError(f"environment variable {dsn_env} is not set")
        return cls(dsn=dsn, timeout_seconds=timeout_seconds)

    @contextmanager
    def connect(self) -> Iterator[Any]:
        try:
            import pymysql
            import pymysql.cursors
        except ModuleNotFoundError as exc:
            raise MissingMySQLDriverError(
                "PyMySQL is required for live MySQL connections; install contextty with runtime dependencies"
            ) from exc

        timeout = max(1, int(self.timeout_seconds))
        timeout_ms = max(1, int(self.timeout_seconds * 1000))
        connect_args = parse_mysql_dsn(self.dsn, allowed_schemes=self.schemes)
        conn = pymysql.connect(
            **connect_args,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
        )
        try:
            with conn.cursor() as cur:
                _execute_best_effort(cur, "SET SESSION TRANSACTION READ ONLY")
                _execute_best_effort(cur, "SET SESSION MAX_EXECUTION_TIME = %s", (timeout_ms,))
                _execute_best_effort(cur, "SET SESSION max_statement_time = %s", (float(self.timeout_seconds),))
            yield conn
        finally:
            conn.close()


class MySQLIntrospector:
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

        database = _first_database_name(table_rows, column_rows, "mysql")
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
        indexes = group_index_rows(index_rows, database)
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
            f"SELECT count(*) AS row_count FROM (SELECT 1 FROM {table_name} LIMIT %s) AS contextty_sample",
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

        timestamp_columns = [column for column in columns if mysql_type_kind(column.data_type) == "time"]
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
        return self._fetchall(conn, f"SELECT {column_sql} FROM {table_name} LIMIT %s", (limit,))

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
                sum(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) AS null_count,
                count(DISTINCT {column_name}) AS distinct_count
            FROM (
                SELECT {column_name}
                FROM {table_name}
                LIMIT %s
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

        data_kind = mysql_type_kind(column.data_type)
        if data_kind in {"numeric", "time"}:
            minmax = self._fetchone(
                conn,
                f"""
                SELECT min({column_name}) AS min_value, max({column_name}) AS max_value
                FROM (
                    SELECT {column_name}
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    LIMIT %s
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
                        LIMIT %s
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
                    LIMIT %s
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
        formats = {
            "hour": "%Y-%m-%dT%H:00:00",
            "day": "%Y-%m-%d",
            "week": "%x-W%v",
            "month": "%Y-%m",
        }
        window = formats.get(options.time_window, formats["day"])
        return [
            {"window_start": row["window_start"], "count": int(row["count"])}
            for row in self._fetchall(
                conn,
                f"""
                SELECT DATE_FORMAT({column_name}, %s) AS window_start, count(*) AS count
                FROM (
                    SELECT {column_name}
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    LIMIT %s
                ) AS contextty_sample
                GROUP BY window_start
                ORDER BY window_start
                LIMIT 50
                """,
                (window, options.row_limit),
            )
            if row["window_start"] is not None
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


def mysql_type_kind(data_type: str) -> str:
    normalized = data_type.lower()
    if normalized in NUMERIC_TYPES:
        return "numeric"
    if normalized in TIME_TYPES or "timestamp" in normalized or "datetime" in normalized:
        return "time"
    if normalized in TEXT_TYPES or "char" in normalized or "text" in normalized:
        return "text"
    return "other"


def group_index_rows(rows: list[dict[str, Any]], database: str | None = None) -> list[IndexInfo]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["schema_name"], row["table_name"], row["index_name"])
        grouped.setdefault(key, []).append(row)

    indexes: list[IndexInfo] = []
    for (schema, table, name), index_rows in sorted(grouped.items()):
        sorted_rows = sorted(index_rows, key=lambda row: int(row["seq_in_index"] or 0))
        columns = [row["column_name"] for row in sorted_rows if row["column_name"]]
        non_unique = bool(int(sorted_rows[0]["non_unique"] or 0))
        indexes.append(
            IndexInfo(
                database=sorted_rows[0].get("database_name") or database or schema,
                schema=schema,
                table=table,
                name=name,
                columns=columns,
                unique=not non_unique,
                primary=name.upper() == "PRIMARY",
                definition=_index_definition(name, columns, unique=not non_unique),
            )
        )
    return indexes


def _query_value(query: dict[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    if not values:
        return None
    return values[0]


def _execute_best_effort(cur: Any, sql: str, params: tuple[Any, ...] | None = None) -> None:
    try:
        cur.execute(sql, params or ())
    except Exception:
        return


def _first_database_name(first: list[dict[str, Any]], second: list[dict[str, Any]], default: str) -> str:
    for rows in (first, second):
        if rows and rows[0].get("database_name"):
            return rows[0]["database_name"]
    return default


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None


def _index_definition(name: str, columns: list[str], unique: bool) -> str:
    prefix = "UNIQUE INDEX" if unique else "INDEX"
    return f"{prefix} {quote_ident(name)} ({', '.join(quote_ident(column) for column in columns)})"
