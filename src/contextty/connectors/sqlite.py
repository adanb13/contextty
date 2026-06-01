from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

from .common import text_patterns
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

SQLITE_SCHEMA = "main"


class SQLiteConnector:
    def __init__(self, path: str | Path, timeout_seconds: float = 5.0) -> None:
        self.path = Path(path).expanduser()
        self.timeout_seconds = timeout_seconds

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        resolved = self.path.resolve()
        uri = f"file:{quote(str(resolved), safe='/:')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=max(0.0, min(self.timeout_seconds, 30.0)))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA query_only = ON")
            yield conn
        finally:
            conn.close()


class SQLiteIntrospector:
    def __init__(self, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds

    def inspect(self, conn: sqlite3.Connection) -> InspectionResult:
        database = self._database_name(conn)
        schema_rows = self._fetchall(
            conn,
            """
            SELECT type, name, sql
            FROM sqlite_schema
            WHERE type IN ('table', 'view')
              AND name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """,
        )

        tables = [
            TableInfo(
                database=database,
                schema=SQLITE_SCHEMA,
                name=row["name"],
                kind=row["type"],
            )
            for row in schema_rows
        ]

        columns: list[ColumnInfo] = []
        primary_keys: list[PrimaryKeyInfo] = []
        foreign_keys: list[ForeignKeyInfo] = []
        indexes: list[IndexInfo] = []
        views: list[ViewInfo] = []

        for row in schema_rows:
            name = row["name"]
            for column, pk_ordinal in self._table_columns(conn, database, name):
                columns.append(column)
                if pk_ordinal:
                    primary_keys.append(
                        PrimaryKeyInfo(
                            database=database,
                            schema=SQLITE_SCHEMA,
                            table=name,
                            column=column.name,
                            ordinal=int(pk_ordinal),
                            constraint_name=f"{name}_pkey",
                        )
                    )

            if row["type"] == "table":
                foreign_keys.extend(self._foreign_keys(conn, database, name))
                indexes.extend(self._indexes(conn, database, name))
            elif row["type"] == "view":
                views.append(
                    ViewInfo(
                        database=database,
                        schema=SQLITE_SCHEMA,
                        name=name,
                        definition=row["sql"],
                    )
                )

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
        conn: sqlite3.Connection,
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

    def execute_readonly_query(self, conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
        validate_readonly_sql(sql)
        return self._fetchall(conn, sql)

    def _table_columns(self, conn: sqlite3.Connection, database: str, table: str) -> list[tuple[ColumnInfo, int]]:
        rows = self._fetchall(conn, f"PRAGMA table_xinfo({quote_string(table)})")
        columns: list[tuple[ColumnInfo, int]] = []
        for row in rows:
            if int(row.get("hidden") or 0):
                continue
            pk_ordinal = int(row["pk"] or 0)
            column = ColumnInfo(
                database=database,
                schema=SQLITE_SCHEMA,
                table=table,
                name=row["name"],
                ordinal=int(row["cid"]) + 1,
                data_type=row["type"] or "unknown",
                nullable=False if pk_ordinal else not bool(row["notnull"]),
                default=row["dflt_value"],
            )
            columns.append((column, pk_ordinal))
        return columns

    def _foreign_keys(self, conn: sqlite3.Connection, database: str, table: str) -> list[ForeignKeyInfo]:
        rows = self._fetchall(conn, f"PRAGMA foreign_key_list({quote_string(table)})")
        return [
            ForeignKeyInfo(
                database=database,
                schema=SQLITE_SCHEMA,
                table=table,
                column=row["from"],
                ref_schema=SQLITE_SCHEMA,
                ref_table=row["table"],
                ref_column=row["to"] or "rowid",
                constraint_name=f"{table}_fk_{row['id']}",
            )
            for row in rows
        ]

    def _indexes(self, conn: sqlite3.Connection, database: str, table: str) -> list[IndexInfo]:
        indexes: list[IndexInfo] = []
        for row in self._fetchall(conn, f"PRAGMA index_list({quote_string(table)})"):
            name = row["name"]
            column_rows = self._fetchall(conn, f"PRAGMA index_info({quote_string(name)})")
            definition_row = self._fetchone(
                conn,
                "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
                (name,),
            )
            origin = row["origin"]
            indexes.append(
                IndexInfo(
                    database=database,
                    schema=SQLITE_SCHEMA,
                    table=table,
                    name=name,
                    columns=[column["name"] for column in column_rows if column["name"] is not None],
                    unique=bool(row["unique"]),
                    primary=origin == "pk",
                    definition=definition_row["sql"] if definition_row else None,
                )
            )
        return indexes

    def _profile_table(
        self,
        conn: sqlite3.Connection,
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

        timestamp_columns = [column for column in columns if sqlite_type_kind(column.data_type) == "time"]
        if timestamp_columns:
            profile.time_windows = self._profile_time_windows(conn, table, timestamp_columns[0], options)
        return profile

    def _profile_column(
        self,
        conn: sqlite3.Connection,
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

        data_kind = sqlite_type_kind(column.data_type)
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
        conn: sqlite3.Connection,
        table: TableInfo,
        column: ColumnInfo,
        options: SnapshotOptions,
    ) -> list[dict[str, Any]]:
        table_name = qualified_table(table.schema, table.name)
        column_name = quote_ident(column.name)
        formats = {
            "hour": "%Y-%m-%dT%H:00:00",
            "day": "%Y-%m-%d",
            "week": "%Y-W%W",
            "month": "%Y-%m",
        }
        window = formats.get(options.time_window, formats["day"])
        return [
            {"window_start": row["window_start"], "count": int(row["count"])}
            for row in self._fetchall(
                conn,
                f"""
                SELECT strftime(?, {column_name}) AS window_start, count(*) AS count
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

    def _database_name(self, conn: sqlite3.Connection) -> str:
        row = self._fetchone(conn, "PRAGMA database_list")
        if not row or not row["file"]:
            return "sqlite"
        return Path(row["file"]).name

    def _fetchall(
        self,
        conn: sqlite3.Connection,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        with self._bounded(conn):
            cursor = conn.execute(sql, params or ())
            return [dict(row) for row in cursor.fetchall()]

    def _fetchone(
        self,
        conn: sqlite3.Connection,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> dict[str, Any] | None:
        rows = self._fetchall(conn, sql, params)
        return rows[0] if rows else None

    @contextmanager
    def _bounded(self, conn: sqlite3.Connection) -> Iterator[None]:
        started_at = time.monotonic()
        timeout_seconds = max(0.001, self.timeout_seconds)

        def progress_handler() -> int:
            return 1 if time.monotonic() - started_at > timeout_seconds else 0

        conn.set_progress_handler(progress_handler, 1000)
        try:
            yield
        except sqlite3.OperationalError as exc:
            if "interrupted" in str(exc).lower():
                raise TimeoutError(f"SQLite query exceeded {timeout_seconds:g}s timeout") from exc
            raise
        finally:
            conn.set_progress_handler(None, 0)


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def sqlite_type_kind(data_type: str) -> str:
    normalized = data_type.upper()
    if any(token in normalized for token in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC", "BOOL")):
        return "numeric"
    if any(token in normalized for token in ("DATE", "TIME")):
        return "time"
    if any(token in normalized for token in ("CHAR", "CLOB", "TEXT", "JSON", "UUID", "VARCHAR")):
        return "text"
    return "other"
