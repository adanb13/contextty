from __future__ import annotations

import hashlib
import re
from dataclasses import asdict
from typing import Any

from .models import (
    ColumnInfo,
    ColumnProfile,
    ForeignKeyInfo,
    IndexInfo,
    InspectionResult,
    SnapshotRun,
    Source,
    TableInfo,
    TableProfile,
)
from .storage import dumps_json


def make_node_id(kind: str, *parts: str) -> str:
    raw = "\x1f".join([kind, *parts])
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    label = re.sub(r"[^A-Za-z0-9_.-]+", "_", ".".join(parts)).strip("._")[:80]
    return f"{kind}:{digest}:{label or kind}"


class ArtifactBuilder:
    def __init__(
        self,
        source: Source,
        run: SnapshotRun,
        inspection: InspectionResult,
        profiles: dict[tuple[str, str], TableProfile] | None = None,
    ) -> None:
        self.source = source
        self.run = run
        self.inspection = inspection
        self.profiles = profiles or {}
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self.pills: list[dict[str, Any]] = []
        self._node_ids: set[str] = set()
        self._schema_ids: dict[str, str] = {}
        self._table_ids: dict[tuple[str, str], str] = {}
        self._column_ids: dict[tuple[str, str, str], str] = {}

    def build(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        source_id = make_node_id("source", self.source.name)
        db_id = make_node_id("database", self.source.name, self.inspection.database)
        source_properties = {"connector_type": self.source.connector_type}
        if self.source.connector_type == "postgres":
            source_properties["dsn_env"] = self.source.dsn_env
        elif self.source.connector_type == "sqlite":
            source_properties["path"] = self.source.path
        self._add_node(source_id, "source", self.source.name, self.source.name, None, "Registered source", source_properties)
        self._add_node(db_id, "database", self.inspection.database, self.inspection.database, source_id, None, {})
        self._add_edge(source_id, db_id, "contains")

        for table in self.inspection.tables:
            self._ensure_schema(db_id, table.schema)
            if table.kind in {"view", "materialized_view"}:
                self._add_view(table)
            else:
                self._add_table(table)

        for view in self.inspection.views:
            self._ensure_schema(db_id, view.schema)
            self._add_view_info(view)

        for column in self.inspection.columns:
            self._add_column(column)

        for index in self.inspection.indexes:
            self._add_index(index)

        for foreign_key in self.inspection.foreign_keys:
            self._add_foreign_key(foreign_key)

        self._add_context_pills()
        return self.nodes, self.edges, self.pills

    def _ensure_schema(self, db_id: str, schema: str) -> str:
        if schema in self._schema_ids:
            return self._schema_ids[schema]
        schema_id = make_node_id("schema", self.source.name, self.inspection.database, schema)
        self._schema_ids[schema] = schema_id
        self._add_node(schema_id, "schema", schema, f"{self.inspection.database}.{schema}", db_id, None, {})
        self._add_edge(db_id, schema_id, "contains")
        return schema_id

    def _add_table(self, table: TableInfo) -> str:
        parent_id = self._schema_ids[table.schema]
        table_id = make_node_id("table", self.source.name, table.database, table.schema, table.name)
        self._table_ids[(table.schema, table.name)] = table_id
        profile = self.profiles.get((table.schema, table.name))
        properties = asdict(table)
        if profile:
            properties["profile"] = asdict(profile)
        summary = summarize_table(table, self._pks_for(table), self._fks_for(table), self._indexes_for(table), profile)
        self._add_node(table_id, "table", table.name, table.qualified_name, parent_id, summary, properties)
        self._add_edge(parent_id, table_id, "contains")
        return table_id

    def _add_view(self, table: TableInfo) -> str:
        parent_id = self._schema_ids[table.schema]
        view_id = make_node_id("view", self.source.name, table.database, table.schema, table.name)
        self._table_ids[(table.schema, table.name)] = view_id
        properties = asdict(table)
        view_info = self._view_for(table)
        if view_info:
            properties["definition"] = view_info.definition
        self._add_node(view_id, "view", table.name, table.qualified_name, parent_id, f"View {table.qualified_name}", properties)
        self._add_edge(parent_id, view_id, "contains")
        return view_id

    def _add_view_info(self, view: Any) -> None:
        if (view.schema, view.name) in self._table_ids:
            return
        parent_id = self._schema_ids[view.schema]
        view_id = make_node_id("view", self.source.name, view.database, view.schema, view.name)
        self._table_ids[(view.schema, view.name)] = view_id
        self._add_node(
            view_id,
            "view",
            view.name,
            f"{view.database}.{view.schema}.{view.name}",
            parent_id,
            f"View {view.database}.{view.schema}.{view.name}",
            asdict(view),
        )
        self._add_edge(parent_id, view_id, "contains")

    def _add_column(self, column: ColumnInfo) -> None:
        table_id = self._table_ids.get((column.schema, column.table))
        if not table_id:
            return
        column_id = make_node_id(
            "column",
            self.source.name,
            column.database,
            column.schema,
            column.table,
            column.name,
        )
        self._column_ids[(column.schema, column.table, column.name)] = column_id
        profile = self.profiles.get((column.schema, column.table), TableProfile()).columns.get(column.name)
        properties = asdict(column)
        if profile:
            properties["profile"] = asdict(profile)
        summary = summarize_column(column, self._is_pk(column), self._fk_for(column), profile)
        self._add_node(column_id, "column", column.name, column.qualified_name, table_id, summary, properties)
        self._add_edge(table_id, column_id, "has_column", {"ordinal": column.ordinal})

    def _add_index(self, index: IndexInfo) -> None:
        table_id = self._table_ids.get((index.schema, index.table))
        if not table_id:
            return
        index_id = make_node_id("index", self.source.name, index.database, index.schema, index.table, index.name)
        self._add_node(
            index_id,
            "index",
            index.name,
            f"{index.database}.{index.schema}.{index.name}",
            table_id,
            summarize_index(index),
            asdict(index),
        )
        self._add_edge(table_id, index_id, "indexed_by", {"columns": index.columns})

    def _add_foreign_key(self, foreign_key: ForeignKeyInfo) -> None:
        from_column = self._column_ids.get((foreign_key.schema, foreign_key.table, foreign_key.column))
        to_column = self._column_ids.get((foreign_key.ref_schema, foreign_key.ref_table, foreign_key.ref_column))
        from_table = self._table_ids.get((foreign_key.schema, foreign_key.table))
        to_table = self._table_ids.get((foreign_key.ref_schema, foreign_key.ref_table))
        properties = asdict(foreign_key)
        if from_column and to_column:
            self._add_edge(from_column, to_column, "foreign_key_to", properties)
        if from_table and to_table:
            self._add_edge(from_table, to_table, "foreign_key_to", properties)

    def _add_context_pills(self) -> None:
        table_by_key = {(table.schema, table.name): table for table in self.inspection.tables}
        column_by_key = {(column.schema, column.table, column.name): column for column in self.inspection.columns}

        for key, table_id in self._table_ids.items():
            table = table_by_key.get(key)
            if not table or table.kind in {"view", "materialized_view"}:
                continue
            profile = self.profiles.get(key)
            data = table_pill_data(table, self._pks_for(table), self._fks_for(table), self._indexes_for(table), profile)
            self._add_pill(table_id, "table_summary", f"Table {table.schema}.{table.name}", data, render_table_pill(data))
            if profile and profile.time_windows:
                data = {
                    "table": table.qualified_name,
                    "windows": profile.time_windows,
                }
                self._add_pill(table_id, "time_window", f"Time windows {table.schema}.{table.name}", data, render_time_window_pill(data))

        for key, column_id in self._column_ids.items():
            column = column_by_key[key]
            profile = self.profiles.get((column.schema, column.table), TableProfile()).columns.get(column.name)
            data = column_pill_data(column, self._is_pk(column), self._fk_for(column), profile)
            self._add_pill(column_id, "column_summary", f"Column {column.schema}.{column.table}.{column.name}", data, render_column_pill(data))
            if profile and profile.patterns:
                pattern_data = {
                    "column": column.qualified_name,
                    "patterns": profile.patterns,
                }
                self._add_pill(column_id, "text_patterns", f"Text patterns {column.schema}.{column.table}.{column.name}", pattern_data, render_pattern_pill(pattern_data))

    def _add_pill(self, node_id: str, kind: str, title: str, data: dict[str, Any], rendered_text: str) -> None:
        pill_id = make_node_id("pill", self.source.name, str(self.run.id), kind, title)
        self.pills.append(
            {
                "id": pill_id,
                "source_id": self.source.id,
                "snapshot_run_id": self.run.id,
                "node_id": node_id,
                "kind": kind,
                "title": title,
                "json": dumps_json(data),
                "rendered_text": rendered_text,
            }
        )
        self._add_node(pill_id, "context_pill", title, title, node_id, rendered_text, {"kind": kind, "data": data})
        self._add_edge(node_id, pill_id, "summarized_by", {"kind": kind})

    def _add_node(
        self,
        node_id: str,
        kind: str,
        name: str,
        qualified_name: str,
        parent_id: str | None,
        summary: str | None,
        properties: dict[str, Any],
    ) -> None:
        if node_id in self._node_ids:
            return
        self._node_ids.add(node_id)
        self.nodes.append(
            {
                "id": node_id,
                "source_id": self.source.id,
                "snapshot_run_id": self.run.id,
                "kind": kind,
                "name": name,
                "qualified_name": qualified_name,
                "parent_id": parent_id,
                "summary": summary,
                "properties_json": dumps_json(properties),
            }
        )

    def _add_edge(
        self,
        from_node_id: str,
        to_node_id: str,
        relation: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        self.edges.append(
            {
                "source_id": self.source.id,
                "snapshot_run_id": self.run.id,
                "from_node_id": from_node_id,
                "to_node_id": to_node_id,
                "relation": relation,
                "properties_json": dumps_json(properties or {}),
            }
        )

    def _pks_for(self, table: TableInfo) -> list[PrimaryKeyInfo]:
        return [pk for pk in self.inspection.primary_keys if pk.schema == table.schema and pk.table == table.name]

    def _fks_for(self, table: TableInfo) -> list[ForeignKeyInfo]:
        return [fk for fk in self.inspection.foreign_keys if fk.schema == table.schema and fk.table == table.name]

    def _indexes_for(self, table: TableInfo) -> list[IndexInfo]:
        return [index for index in self.inspection.indexes if index.schema == table.schema and index.table == table.name]

    def _view_for(self, table: TableInfo) -> Any | None:
        for view in self.inspection.views:
            if view.schema == table.schema and view.name == table.name:
                return view
        return None

    def _is_pk(self, column: ColumnInfo) -> bool:
        return any(
            pk.schema == column.schema and pk.table == column.table and pk.column == column.name
            for pk in self.inspection.primary_keys
        )

    def _fk_for(self, column: ColumnInfo) -> ForeignKeyInfo | None:
        for fk in self.inspection.foreign_keys:
            if fk.schema == column.schema and fk.table == column.table and fk.column == column.name:
                return fk
        return None


def build_artifact(
    source: Source,
    run: SnapshotRun,
    inspection: InspectionResult,
    profiles: dict[tuple[str, str], TableProfile] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return ArtifactBuilder(source, run, inspection, profiles).build()


def table_pill_data(
    table: TableInfo,
    pks: list[PrimaryKeyInfo],
    fks: list[ForeignKeyInfo],
    indexes: list[IndexInfo],
    profile: TableProfile | None,
) -> dict[str, Any]:
    return {
        "table": table.qualified_name,
        "kind": table.kind,
        "row_estimate": table.row_estimate,
        "row_count": profile.row_count if profile else table.row_estimate,
        "row_count_is_capped": profile.row_count_is_capped if profile else False,
        "size_bytes": table.size_bytes,
        "primary_keys": [pk.column for pk in pks],
        "foreign_keys": [
            {
                "column": fk.column,
                "references": f"{fk.ref_schema}.{fk.ref_table}.{fk.ref_column}",
                "constraint": fk.constraint_name,
            }
            for fk in fks
        ],
        "indexes": [
            {
                "name": index.name,
                "columns": index.columns,
                "unique": index.unique,
                "primary": index.primary,
            }
            for index in indexes[:10]
        ],
        "top_columns": list((profile.columns if profile else {}).keys())[:20],
    }


def column_pill_data(
    column: ColumnInfo,
    is_pk: bool,
    fk: ForeignKeyInfo | None,
    profile: ColumnProfile | None,
) -> dict[str, Any]:
    return {
        "column": column.qualified_name,
        "type": column.data_type,
        "nullable": column.nullable,
        "default": column.default,
        "primary_key": is_pk,
        "foreign_key": (
            {
                "references": f"{fk.ref_schema}.{fk.ref_table}.{fk.ref_column}",
                "constraint": fk.constraint_name,
            }
            if fk
            else None
        ),
        "profile": asdict(profile) if profile else {},
    }


def summarize_table(
    table: TableInfo,
    pks: list[PrimaryKeyInfo],
    fks: list[ForeignKeyInfo],
    indexes: list[IndexInfo],
    profile: TableProfile | None,
) -> str:
    row_count = profile.row_count if profile and profile.row_count is not None else table.row_estimate
    bits = [f"{table.kind.title()} {table.qualified_name}"]
    if row_count is not None:
        suffix = " sampled/capped" if profile and profile.row_count_is_capped else ""
        bits.append(f"rows={row_count}{suffix}")
    if pks:
        bits.append("pk=" + ",".join(pk.column for pk in pks))
    if fks:
        bits.append(f"foreign_keys={len(fks)}")
    if indexes:
        bits.append(f"indexes={len(indexes)}")
    return "; ".join(bits)


def summarize_column(
    column: ColumnInfo,
    is_pk: bool,
    fk: ForeignKeyInfo | None,
    profile: ColumnProfile | None,
) -> str:
    bits = [f"Column {column.qualified_name}", column.data_type]
    bits.append("nullable" if column.nullable else "not null")
    if is_pk:
        bits.append("primary key")
    if fk:
        bits.append(f"references {fk.ref_schema}.{fk.ref_table}.{fk.ref_column}")
    if profile and profile.null_rate is not None:
        bits.append(f"null_rate={profile.null_rate:.3f}")
    if profile and profile.distinct_count is not None:
        bits.append(f"distinct={profile.distinct_count}")
    return "; ".join(bits)


def summarize_index(index: IndexInfo) -> str:
    flags = []
    if index.primary:
        flags.append("primary")
    if index.unique:
        flags.append("unique")
    return f"Index {index.name} on {index.schema}.{index.table} ({', '.join(index.columns)}) {' '.join(flags)}".strip()


def render_table_pill(data: dict[str, Any]) -> str:
    rows = data.get("row_count")
    if rows is None:
        rows = data.get("row_estimate")
    parts = [f"{data['table']} is a {data['kind']}"]
    if rows is not None:
        parts.append(f"with {rows} rows" + (" in the bounded sample" if data.get("row_count_is_capped") else ""))
    if data.get("primary_keys"):
        parts.append("primary key " + ", ".join(data["primary_keys"]))
    if data.get("foreign_keys"):
        refs = ", ".join(f"{fk['column']} -> {fk['references']}" for fk in data["foreign_keys"][:5])
        parts.append("foreign keys " + refs)
    if data.get("indexes"):
        parts.append(f"{len(data['indexes'])} indexed access paths")
    return "; ".join(parts) + "."


def render_column_pill(data: dict[str, Any]) -> str:
    parts = [f"{data['column']} is {data['type']}"]
    parts.append("nullable" if data["nullable"] else "not nullable")
    if data.get("primary_key"):
        parts.append("primary key")
    if data.get("foreign_key"):
        parts.append("references " + data["foreign_key"]["references"])
    profile = data.get("profile") or {}
    if profile.get("null_rate") is not None:
        parts.append(f"null rate {profile['null_rate']:.3f}")
    if profile.get("distinct_count") is not None:
        parts.append(f"{profile['distinct_count']} distinct sampled values")
    return "; ".join(parts) + "."


def render_pattern_pill(data: dict[str, Any]) -> str:
    patterns = ", ".join(f"{item['template']} ({item['count']})" for item in data.get("patterns", [])[:5])
    return f"{data['column']} text patterns: {patterns}."


def render_time_window_pill(data: dict[str, Any]) -> str:
    windows = data.get("windows", [])
    if not windows:
        return f"{data['table']} has no sampled time windows."
    first = windows[0]["window_start"]
    last = windows[-1]["window_start"]
    total = sum(int(item["count"]) for item in windows)
    return f"{data['table']} has {total} sampled rows across {len(windows)} time windows from {first} to {last}."
