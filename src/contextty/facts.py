from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import asdict
from typing import Any

from .models import (
    ColumnInfo,
    ForeignKeyInfo,
    InspectionResult,
    PrimaryKeyInfo,
    SnapshotOptions,
    SnapshotRun,
    Source,
    TableInfo,
)

SCHEMA_FACT_KINDS = {
    "column_group",
    "relationship_card",
    "table_inventory",
    "table_schema",
    "value_domain",
}
ROW_FACT_KINDS = {
    "aggregate",
    "bridge",
    "entity",
    "latest_metric",
    "relationship",
}
ANSWER_FACT_KINDS = SCHEMA_FACT_KINDS | ROW_FACT_KINDS

MAX_SOURCE_ROWS_PER_TABLE = 500
MAX_FACTS_PER_TABLE = 200
MAX_ROW_FACTS = 2000
MAX_TEXT_VALUE_LENGTH = 80
HASH_VECTOR_DIMS = 64

LABEL_COLUMN_SETS = (
    ("first_name", "last_name"),
    ("name",),
    ("title",),
    ("label",),
    ("city", "state"),
    ("code",),
)
FREE_TEXT_TOKENS = {
    "address",
    "bio",
    "body",
    "comment",
    "description",
    "email",
    "message",
    "notes",
    "summary",
}
DOMAIN_TOKENS = {
    "currency",
    "level",
    "period",
    "reason",
    "role",
    "state",
    "status",
    "type",
}
COMPACT_TEXT_ATTRIBUTE_TOKENS = {"center", "code", "country", "currency", "level", "number", "reason", "role", "state", "status", "type"}
COMPACT_IDENTIFIER_TOKENS = {"code", "identifier", "number", "ref", "reference"}
DATE_TOKENS = {"date", "time", "created", "completed", "effective", "ended", "opened", "started", "submitted"}
ID_TOKENS = {"id", "key"}
STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "by",
    "for",
    "from",
    "has",
    "have",
    "how",
    "in",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "what",
    "which",
    "who",
    "with",
}


def tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for token in re.findall(r"[A-Za-z0-9_]+", text.lower()):
        if len(token) > 1 and token not in STOP_WORDS:
            tokens.append(token)
            if token.endswith("s") and len(token) > 3:
                tokens.append(token[:-1])
        for part in token.split("_"):
            if len(part) > 1 and part not in STOP_WORDS:
                tokens.append(part)
                if part.endswith("s") and len(part) > 3:
                    tokens.append(part[:-1])
    return tokens


def hashed_vector(text: str, dims: int = HASH_VECTOR_DIMS) -> list[float]:
    vector = [0.0] * dims
    for token in tokenize(text):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        bucket = int.from_bytes(digest[:4], "big") % dims
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if not norm:
        return vector
    return [round(value / norm, 6) for value in vector]


def vector_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    size = min(len(left), len(right))
    return sum(left[index] * right[index] for index in range(size))


def prepare_fact_for_storage(fact: dict[str, Any]) -> dict[str, Any]:
    text = str(fact.get("text") or "")
    search_text = str(fact.get("search_text") or text)
    data = fact.get("data")
    if isinstance(data, str):
        data_json = data
    else:
        data_json = json.dumps(data or {}, sort_keys=True, default=str)
    vector = fact.get("vector")
    if not isinstance(vector, list):
        vector = hashed_vector(search_text)
    return {
        "id": fact["id"],
        "source_id": fact["source_id"],
        "snapshot_run_id": fact["snapshot_run_id"],
        "node_id": fact.get("node_id"),
        "kind": fact["kind"],
        "subject": fact["subject"],
        "text": text,
        "data_json": data_json,
        "search_text": search_text,
        "vector_json": json.dumps(vector, separators=(",", ":")),
    }


def make_fact_id(source: Source, run: SnapshotRun, kind: str, subject: str, text: str = "") -> str:
    raw = "\x1f".join([source.name, str(run.id), kind, subject, text])
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    label = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{kind}.{subject}").strip("._")[:90]
    return f"fact:{digest}:{label or kind}"


def facts_from_pills(pills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for pill in pills:
        if pill.get("kind") not in SCHEMA_FACT_KINDS:
            continue
        data = pill.get("json") or {}
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                data = {}
        text = pill.get("rendered_text") or ""
        search_text = " ".join([str(pill.get("kind") or ""), str(pill.get("title") or ""), text, flatten_for_search(data)])
        digest = hashlib.sha1(str(pill["id"]).encode("utf-8")).hexdigest()[:16]
        facts.append(
            prepare_fact_for_storage(
                {
                    "id": f"fact:{digest}:{str(pill['id'])[:90]}",
                    "source_id": pill["source_id"],
                    "snapshot_run_id": pill["snapshot_run_id"],
                    "node_id": pill.get("node_id"),
                    "kind": pill["kind"],
                    "subject": pill.get("title") or pill["kind"],
                    "text": text,
                    "data": data,
                    "search_text": search_text,
                }
            )
        )
    return facts


def build_row_facts(
    source: Source,
    run: SnapshotRun,
    inspection: InspectionResult,
    rows_by_table: dict[tuple[str, str], list[dict[str, Any]]],
    options: SnapshotOptions,
) -> list[dict[str, Any]]:
    if options.profile_mode != "deep":
        return []

    context = RowFactContext(source, run, inspection, rows_by_table)
    collector = FactCollector(source, run)
    add_entity_facts(context, collector)
    add_relationship_facts(context, collector)
    add_bridge_facts(context, collector)
    add_earliest_domain_date_facts(context, collector)
    add_latest_metric_facts(context, collector)
    add_bridge_sum_facts(context, collector)
    add_grouped_average_facts(context, collector)
    return collector.facts


class FactCollector:
    def __init__(self, source: Source, run: SnapshotRun) -> None:
        self.source = source
        self.run = run
        self.facts: list[dict[str, Any]] = []
        self._per_table: dict[tuple[str, str], int] = defaultdict(int)

    def add(
        self,
        kind: str,
        subject: str,
        text: str,
        data: dict[str, Any],
        table_key: tuple[str, str] | None = None,
    ) -> None:
        if len(self.facts) >= MAX_ROW_FACTS:
            return
        if table_key is not None:
            if self._per_table[table_key] >= MAX_FACTS_PER_TABLE:
                return
            self._per_table[table_key] += 1
        fact = prepare_fact_for_storage(
            {
                "id": make_fact_id(self.source, self.run, kind, subject, text),
                "source_id": self.source.id,
                "snapshot_run_id": self.run.id,
                "kind": kind,
                "subject": subject,
                "text": text,
                "data": data,
                "search_text": " ".join([kind, subject, text, flatten_for_search(data)]),
            }
        )
        self.facts.append(fact)


class RowFactContext:
    def __init__(
        self,
        source: Source,
        run: SnapshotRun,
        inspection: InspectionResult,
        rows_by_table: dict[tuple[str, str], list[dict[str, Any]]],
    ) -> None:
        self.source = source
        self.run = run
        self.inspection = inspection
        self.rows_by_table = {
            key: rows[:MAX_SOURCE_ROWS_PER_TABLE]
            for key, rows in rows_by_table.items()
            if rows is not None
        }
        self.tables = {(table.schema, table.name): table for table in inspection.tables}
        self.columns_by_table: dict[tuple[str, str], list[ColumnInfo]] = defaultdict(list)
        for column in inspection.columns:
            self.columns_by_table[(column.schema, column.table)].append(column)
        self.primary_keys_by_table: dict[tuple[str, str], list[PrimaryKeyInfo]] = defaultdict(list)
        for pk in inspection.primary_keys:
            self.primary_keys_by_table[(pk.schema, pk.table)].append(pk)
        self.foreign_keys_by_table: dict[tuple[str, str], list[ForeignKeyInfo]] = defaultdict(list)
        for fk in inspection.foreign_keys:
            self.foreign_keys_by_table[(fk.schema, fk.table)].append(fk)
        self.row_lookup: dict[tuple[str, str], dict[tuple[Any, ...], dict[str, Any]]] = {}
        self.row_lookup_by_column: dict[tuple[str, str, str], dict[Any, dict[str, Any]]] = {}
        self.dynamic_label_columns_by_table: dict[tuple[str, str], list[str]] = {}
        for key, rows in self.rows_by_table.items():
            for column in self.columns_by_table.get(key, []):
                by_column: dict[Any, dict[str, Any]] = {}
                for row in rows:
                    if row.get(column.name) is not None:
                        by_column[row[column.name]] = row
                self.row_lookup_by_column[(key[0], key[1], column.name)] = by_column
            pk_cols = self.pk_columns(key)
            if not pk_cols:
                continue
            lookup: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in rows:
                row_key = tuple(row.get(column) for column in pk_cols)
                if all(value is not None for value in row_key):
                    lookup[row_key] = row
            self.row_lookup[key] = lookup
            self.dynamic_label_columns_by_table[key] = self._detect_dynamic_label_columns(key)

    def pk_columns(self, table_key: tuple[str, str]) -> list[str]:
        pks = sorted(self.primary_keys_by_table.get(table_key, []), key=lambda pk: pk.ordinal)
        if pks:
            return [pk.column for pk in pks]
        if any(column.name == "id" for column in self.columns_by_table.get(table_key, [])):
            return ["id"]
        return []

    def column_names(self, table_key: tuple[str, str]) -> set[str]:
        return {column.name for column in self.columns_by_table.get(table_key, [])}

    def label(self, table_key: tuple[str, str], row: dict[str, Any] | None) -> str | None:
        if not row:
            return None
        names = self.column_names(table_key)
        for column_set in LABEL_COLUMN_SETS:
            if all(column in names and row.get(column) not in (None, "") for column in column_set):
                return truncate_value(" ".join(str(row[column]) for column in column_set))
        for column in self.dynamic_label_columns_by_table.get(table_key, []):
            value = row.get(column)
            if value not in (None, ""):
                return truncate_value(str(value))
        pk_cols = self.pk_columns(table_key)
        if pk_cols and all(row.get(column) is not None for column in pk_cols):
            return ",".join(str(row[column]) for column in pk_cols)
        return None

    def _detect_dynamic_label_columns(self, table_key: tuple[str, str]) -> list[str]:
        rows = self.rows_by_table.get(table_key, [])
        if not rows:
            return []
        pk_columns = set(self.pk_columns(table_key))
        fk_columns = {fk.column for fk in self.foreign_keys_by_table.get(table_key, [])}
        candidates: list[tuple[int, str]] = []
        for column in self.columns_by_table.get(table_key, []):
            if column.name in pk_columns or column.name in fk_columns:
                continue
            if not is_dynamic_label_column(column):
                continue
            values = [str(row[column.name]).strip() for row in rows if row.get(column.name) not in (None, "")]
            if not values:
                continue
            unique_values = set(values)
            uniqueness = len(unique_values) / len(values)
            if uniqueness < 0.9:
                continue
            if max(len(value) for value in values) > MAX_TEXT_VALUE_LENGTH:
                continue
            candidates.append((dynamic_label_preference(column.name), column.name))
        return [name for _score, name in sorted(candidates)[:3]]

    def target_row(self, fk: ForeignKeyInfo, row: dict[str, Any]) -> dict[str, Any] | None:
        value = row.get(fk.column)
        if value is None:
            return None
        by_ref_column = self.row_lookup_by_column.get((fk.ref_schema, fk.ref_table, fk.ref_column), {})
        if value in by_ref_column:
            return by_ref_column[value]
        return self.row_lookup.get((fk.ref_schema, fk.ref_table), {}).get((value,))

    def table_label(self, table_key: tuple[str, str]) -> str:
        return short_table_name(*table_key)


def sample_columns_for_table(
    inspection: InspectionResult,
    table: TableInfo,
    max_columns: int = 32,
) -> list[str]:
    columns = [column for column in inspection.columns if column.schema == table.schema and column.table == table.name]
    pk_columns = {pk.column for pk in inspection.primary_keys if pk.schema == table.schema and pk.table == table.name}
    fk_columns = {fk.column for fk in inspection.foreign_keys if fk.schema == table.schema and fk.table == table.name}
    names = {column.name for column in columns}
    label_columns: set[str] = set()
    for column_set in LABEL_COLUMN_SETS:
        if all(column in names for column in column_set):
            label_columns.update(column_set)
            break

    selected: list[str] = []
    for column in columns:
        if column.name in pk_columns or column.name in fk_columns or column.name in label_columns or is_dynamic_label_column(column):
            selected.append(column.name)
            continue
        if is_free_text_column(column) and column.name not in label_columns:
            continue
        if is_numeric_column(column) or is_date_column(column) or is_domain_column(column) or is_compact_text_attribute_column(column):
            selected.append(column.name)
    return selected[:max_columns]


def add_entity_facts(context: RowFactContext, collector: FactCollector) -> None:
    for table_key, rows in sorted(context.rows_by_table.items()):
        if len(rows) > MAX_SOURCE_ROWS_PER_TABLE:
            continue
        for row in rows:
            label = context.label(table_key, row)
            if not label:
                continue
            attrs = selected_attributes(context, table_key, row, include_foreign_keys=True)
            text = f"entity {context.table_label(table_key)} {label}: {render_attributes(attrs)}."
            collector.add(
                "entity",
                f"{context.table_label(table_key)} {label}",
                text,
                {
                    "table": context.table_label(table_key),
                    "label": label,
                    "attributes": attrs,
                    "primary_key": pk_data(context, table_key, row),
                },
                table_key,
            )


def add_relationship_facts(context: RowFactContext, collector: FactCollector) -> None:
    grouped_self_fk: dict[tuple[str, str, str], dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    grouped_fk: dict[tuple[str, str, str, str, str], dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for table_key, fks in sorted(context.foreign_keys_by_table.items()):
        rows = context.rows_by_table.get(table_key, [])
        for fk in fks:
            target_key = (fk.ref_schema, fk.ref_table)
            for row in rows:
                source_label = context.label(table_key, row)
                target = context.target_row(fk, row)
                target_label = context.label(target_key, target)
                if not source_label or not target_label:
                    continue
                source_attrs = selected_attributes(context, table_key, row, include_foreign_keys=False, limit=6)
                grouped_fk[(fk.schema, fk.table, fk.column, fk.ref_schema, fk.ref_table)][target_label].append(
                    f"{source_label} {render_attributes(source_attrs)}".strip()
                )
                target_attrs = selected_attributes(context, target_key, target or {}, include_foreign_keys=False, limit=4)
                if fk.table == fk.ref_table:
                    text = (
                        f"self_reference {context.table_label(table_key)}.{fk.column}: "
                        f"{source_label} references {target_label} in {context.table_label(target_key)}; "
                        f"source={source_label}; target={target_label}."
                    )
                    grouped_self_fk[(fk.schema, fk.table, fk.column)][target_label].append(source_label)
                else:
                    suffix = f"; {render_attributes(target_attrs)}" if target_attrs else ""
                    text = (
                        f"relationship {context.table_label(table_key)}.{fk.column}: "
                        f"{source_label} references {target_label} in {context.table_label(target_key)}{suffix}."
                    )
                collector.add(
                    "relationship",
                    f"{context.table_label(table_key)}.{fk.column} {source_label}",
                    text,
                    {
                        "table": context.table_label(table_key),
                        "column": fk.column,
                        "source_label": source_label,
                        "target_table": context.table_label(target_key),
                        "target_label": target_label,
                        "target_attributes": target_attrs,
                    },
                table_key,
            )

    for (schema, table, column, ref_schema, ref_table), rows_by_target in grouped_fk.items():
        table_key = (schema, table)
        target_table = short_table_name(ref_schema, ref_table)
        if schema == ref_schema and table == ref_table:
            continue
        for target_label, row_labels in sorted(rows_by_target.items()):
            if not row_labels:
                continue
            text = (
                f"related_rows {short_table_name(schema, table)}.{column}: "
                f"{target_label} has {short_table_name(schema, table)} rows "
                + "; ".join(row_labels[:20])
                + "."
            )
            collector.add(
                "relationship",
                f"{short_table_name(schema, table)}.{column} rows for {target_label}",
                text,
                {
                    "table": short_table_name(schema, table),
                    "column": column,
                    "target_table": target_table,
                    "target_label": target_label,
                    "rows": row_labels[:20],
                },
                table_key,
            )

    for (schema, table, column), sources_by_target in grouped_self_fk.items():
        table_key = (schema, table)
        for target_label, source_labels in sorted(sources_by_target.items()):
            source_labels = sorted(source_labels)
            text = (
                f"referenced_by {context.table_label(table_key)}.{column}: "
                f"{target_label} is referenced by {', '.join(source_labels)}."
            )
            collector.add(
                "relationship",
                f"{context.table_label(table_key)}.{column} referenced by {target_label}",
                text,
                {
                    "table": context.table_label(table_key),
                    "column": column,
                    "target_label": target_label,
                    "source_labels": source_labels,
                },
                table_key,
            )


def add_bridge_facts(context: RowFactContext, collector: FactCollector) -> None:
    for table_key, fks in sorted(context.foreign_keys_by_table.items()):
        if len(fks) < 2:
            continue
        rows = context.rows_by_table.get(table_key, [])
        endpoint_rows: list[tuple[dict[str, Any], list[tuple[ForeignKeyInfo, tuple[str, str], str]]]] = []
        for row in rows:
            endpoints: list[tuple[ForeignKeyInfo, tuple[str, str], str]] = []
            for fk in fks:
                target_key = (fk.ref_schema, fk.ref_table)
                label = context.label(target_key, context.target_row(fk, row))
                if label:
                    endpoints.append((fk, target_key, label))
            if len(endpoints) >= 2:
                endpoint_rows.append((row, endpoints))
                attrs = selected_attributes(context, table_key, row, include_foreign_keys=False, limit=8)
                text = (
                    f"bridge_assignment {context.table_label(table_key)}: "
                    + " links ".join(f"{context.table_label(target_key)}={label}" for _fk, target_key, label in endpoints)
                    + f"; {render_attributes(attrs)}."
                )
                collector.add(
                    "bridge",
                    f"{context.table_label(table_key)} {' '.join(label for _fk, _target, label in endpoints)}",
                    text,
                    {
                        "table": context.table_label(table_key),
                        "endpoints": [
                            {"column": fk.column, "table": context.table_label(target_key), "label": label}
                            for fk, target_key, label in endpoints
                        ],
                        "attributes": attrs,
                    },
                    table_key,
                )

        for fk, target_key in [(fk, (fk.ref_schema, fk.ref_table)) for fk in fks]:
            grouped: dict[str, list[str]] = defaultdict(list)
            for row, endpoints in endpoint_rows:
                group_label = next((label for endpoint_fk, _target, label in endpoints if endpoint_fk.column == fk.column), None)
                if not group_label:
                    continue
                attrs = selected_attributes(context, table_key, row, include_foreign_keys=False, limit=5)
                other_labels = [
                    label
                    for endpoint_fk, _target, label in endpoints
                    if endpoint_fk.column != fk.column
                ]
                if not other_labels:
                    continue
                grouped[group_label].append(f"{', '.join(other_labels)} {render_attributes(attrs)}".strip())
            for group_label, assignments in sorted(grouped.items()):
                text = (
                    f"assignments for {context.table_label(target_key)} {group_label} via {context.table_label(table_key)}: "
                    + "; ".join(assignments[:20])
                    + "."
                )
                collector.add(
                    "bridge",
                    f"{context.table_label(table_key)} assignments {group_label}",
                    text,
                    {
                        "table": context.table_label(table_key),
                        "group_table": context.table_label(target_key),
                        "group_label": group_label,
                        "assignments": assignments[:20],
                    },
                    table_key,
                )


def add_earliest_domain_date_facts(context: RowFactContext, collector: FactCollector) -> None:
    for table_key, rows in sorted(context.rows_by_table.items()):
        if not rows:
            continue
        columns = context.columns_by_table.get(table_key, [])
        date_columns = [column.name for column in columns if is_date_column(column)]
        domain_columns = [column.name for column in columns if is_domain_column(column)]
        if not date_columns:
            continue
        preferred_dates = sorted(date_columns, key=date_preference)
        for date_column in preferred_dates[:2]:
            candidates = [row for row in rows if row.get(date_column) not in (None, "")]
            if not candidates:
                continue
            for domain_column in domain_columns[:3]:
                values = sorted({row.get(domain_column) for row in candidates if row.get(domain_column) not in (None, "")})
                if not values or len(values) > 12:
                    continue
                for value in values:
                    domain_rows = [row for row in candidates if row.get(domain_column) == value]
                    if not domain_rows:
                        continue
                    earliest = min(domain_rows, key=lambda row: str(row.get(date_column)))
                    label = context.label(table_key, earliest)
                    if not label:
                        continue
                    text = (
                        f"earliest {context.table_label(table_key)}.{date_column} where {domain_column}={format_value(value)}: "
                        f"{label} {date_column}={format_value(earliest.get(date_column))}."
                    )
                    collector.add(
                        "aggregate",
                        f"earliest {context.table_label(table_key)}.{date_column} {domain_column} {value}",
                        text,
                        {
                            "table": context.table_label(table_key),
                            "date_column": date_column,
                            "domain_column": domain_column,
                            "domain_value": value,
                            "label": label,
                            "value": earliest.get(date_column),
                        },
                        table_key,
                    )


def add_latest_metric_facts(context: RowFactContext, collector: FactCollector) -> None:
    for table_key, rows in sorted(context.rows_by_table.items()):
        if not rows:
            continue
        fks = context.foreign_keys_by_table.get(table_key, [])
        if not fks:
            continue
        columns = context.columns_by_table.get(table_key, [])
        date_column = first_date_column(columns)
        numeric_columns = measure_columns(context, table_key)
        if not date_column or not numeric_columns:
            continue
        for fk in fks:
            target_key = (fk.ref_schema, fk.ref_table)
            grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                if row.get(fk.column) is not None and row.get(date_column) not in (None, ""):
                    grouped[row[fk.column]].append(row)
            for group_rows in grouped.values():
                latest = max(group_rows, key=lambda row: str(row.get(date_column)))
                target_label = context.label(target_key, context.target_row(fk, latest))
                if not target_label:
                    continue
                attrs = selected_attributes(context, table_key, latest, include_foreign_keys=False, limit=8)
                text = (
                    f"latest {context.table_label(table_key)} for {target_label} by {date_column}: "
                    f"{render_attributes(attrs)}."
                )
                collector.add(
                    "latest_metric",
                    f"latest {context.table_label(table_key)} {target_label}",
                    text,
                    {
                        "table": context.table_label(table_key),
                        "target_table": context.table_label(target_key),
                        "target_label": target_label,
                        "date_column": date_column,
                        "attributes": attrs,
                    },
                    table_key,
                )


def add_bridge_sum_facts(context: RowFactContext, collector: FactCollector) -> None:
    for table_key, rows in sorted(context.rows_by_table.items()):
        fks = context.foreign_keys_by_table.get(table_key, [])
        if len(fks) < 2 or not rows:
            continue
        numeric_columns = measure_columns(context, table_key)
        for measure in numeric_columns:
            for fk in fks:
                target_key = (fk.ref_schema, fk.ref_table)
                totals: dict[str, float] = defaultdict(float)
                for row in rows:
                    target_label = context.label(target_key, context.target_row(fk, row))
                    value = numeric_value(row.get(measure))
                    if target_label and value is not None:
                        totals[target_label] += value
                if not totals:
                    continue
                ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
                top_label, top_total = ordered[0]
                rendered = "; ".join(f"{label}={format_number(total)}" for label, total in ordered[:10])
                text = (
                    f"top summed {context.table_label(table_key)}.{measure} by {context.table_label(target_key)}: "
                    f"{top_label} total_{measure}={format_number(top_total)}; totals: {rendered}."
                )
                collector.add(
                    "aggregate",
                    f"top sum {context.table_label(table_key)}.{measure} by {context.table_label(target_key)}",
                    text,
                    {
                        "table": context.table_label(table_key),
                        "measure": measure,
                        "group_table": context.table_label(target_key),
                        "top": {"label": top_label, "total": top_total},
                        "totals": dict(ordered[:20]),
                    },
                    table_key,
                )


def add_grouped_average_facts(context: RowFactContext, collector: FactCollector) -> None:
    for table_key, rows in sorted(context.rows_by_table.items()):
        fks = context.foreign_keys_by_table.get(table_key, [])
        numeric_columns = measure_columns(context, table_key)
        if not rows or not fks or not numeric_columns:
            continue
        columns = context.columns_by_table.get(table_key, [])
        date_column = first_date_column(columns)
        domain_columns = [column.name for column in columns if is_domain_column(column)]
        for fk in fks:
            entity_key = (fk.ref_schema, fk.ref_table)
            dimensions = context.foreign_keys_by_table.get(entity_key, [])
            if not dimensions:
                continue
            for dimension_fk in dimensions:
                dimension_key = (dimension_fk.ref_schema, dimension_fk.ref_table)
                for measure in numeric_columns:
                    averages = average_by_dimension(context, rows, fk, dimension_fk, measure)
                    if averages:
                        text = (
                            f"average {context.table_label(table_key)}.{measure} by {context.table_label(dimension_key)} "
                            f"via {context.table_label(entity_key)}.{dimension_fk.column}: "
                            + render_mapping(averages)
                            + "."
                        )
                        collector.add(
                            "aggregate",
                            f"average {context.table_label(table_key)}.{measure} by {context.table_label(dimension_key)}",
                            text,
                            {
                                "table": context.table_label(table_key),
                                "measure": measure,
                                "group_table": context.table_label(dimension_key),
                                "averages": averages,
                            },
                            table_key,
                        )
                    if date_column:
                        latest_rows = latest_rows_by_key(rows, fk.column, date_column)
                        averages = average_by_dimension(context, latest_rows, fk, dimension_fk, measure)
                        if averages:
                            text = (
                                f"average latest {context.table_label(table_key)}.{measure} by {context.table_label(dimension_key)} "
                                f"via {context.table_label(entity_key)}.{dimension_fk.column}: "
                                + render_mapping(averages)
                                + "."
                            )
                            collector.add(
                                "aggregate",
                                f"average latest {context.table_label(table_key)}.{measure} by {context.table_label(dimension_key)}",
                                text,
                                {
                                    "table": context.table_label(table_key),
                                    "measure": measure,
                                    "date_column": date_column,
                                    "group_table": context.table_label(dimension_key),
                                    "averages": averages,
                                },
                                table_key,
                            )
                    for domain_column in domain_columns[:3]:
                        values = sorted({row.get(domain_column) for row in rows if row.get(domain_column) not in (None, "")})
                        if not values or len(values) > 12:
                            continue
                        for value in values:
                            domain_rows = [row for row in rows if row.get(domain_column) == value]
                            averages = average_by_dimension(context, domain_rows, fk, dimension_fk, measure)
                            if not averages:
                                continue
                            text = (
                                f"average {context.table_label(table_key)}.{measure} for {domain_column}={format_value(value)} "
                                f"by {context.table_label(dimension_key)} via {context.table_label(entity_key)}.{dimension_fk.column}: "
                                + render_mapping(averages)
                                + "."
                            )
                            collector.add(
                                "aggregate",
                                f"average {context.table_label(table_key)}.{measure} {domain_column} {value} by {context.table_label(dimension_key)}",
                                text,
                                {
                                    "table": context.table_label(table_key),
                                    "measure": measure,
                                    "domain_column": domain_column,
                                    "domain_value": value,
                                    "group_table": context.table_label(dimension_key),
                                    "averages": averages,
                                },
                                table_key,
                            )


def average_by_dimension(
    context: RowFactContext,
    rows: list[dict[str, Any]],
    entity_fk: ForeignKeyInfo,
    dimension_fk: ForeignKeyInfo,
    measure: str,
) -> dict[str, float]:
    grouped: dict[str, list[float]] = defaultdict(list)
    entity_key = (entity_fk.ref_schema, entity_fk.ref_table)
    dimension_key = (dimension_fk.ref_schema, dimension_fk.ref_table)
    for row in rows:
        entity = context.target_row(entity_fk, row)
        if not entity:
            continue
        dimension = context.target_row(dimension_fk, entity)
        label = context.label(dimension_key, dimension)
        value = numeric_value(row.get(measure))
        if label and value is not None:
            grouped[label].append(value)
    return {
        label: round(sum(values) / len(values), 2)
        for label, values in sorted(grouped.items())
        if values
    }


def latest_rows_by_key(rows: list[dict[str, Any]], key_column: str, date_column: str) -> list[dict[str, Any]]:
    latest: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = row.get(key_column)
        date_value = row.get(date_column)
        if key is None or date_value in (None, ""):
            continue
        if key not in latest or str(date_value) > str(latest[key].get(date_column)):
            latest[key] = row
    return list(latest.values())


def selected_attributes(
    context: RowFactContext,
    table_key: tuple[str, str],
    row: dict[str, Any],
    include_foreign_keys: bool,
    limit: int = 10,
) -> dict[str, Any]:
    pk_columns = set(context.pk_columns(table_key))
    fk_columns = {fk.column for fk in context.foreign_keys_by_table.get(table_key, [])}
    names = context.column_names(table_key)
    label_columns: set[str] = set()
    for column_set in LABEL_COLUMN_SETS:
        if all(column in names for column in column_set):
            label_columns.update(column_set)
            break
    attrs: dict[str, Any] = {}
    for column in context.columns_by_table.get(table_key, []):
        if column.name not in row:
            continue
        if column.name in pk_columns or column.name in label_columns:
            continue
        if not include_foreign_keys and column.name in fk_columns:
            continue
        if is_free_text_column(column):
            continue
        value = row.get(column.name)
        if value is None:
            continue
        attrs[column.name] = truncate_value(value)
        if len(attrs) >= limit:
            break
    return attrs


def pk_data(context: RowFactContext, table_key: tuple[str, str], row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in context.pk_columns(table_key) if column in row}


def measure_columns(context: RowFactContext, table_key: tuple[str, str]) -> list[str]:
    fk_columns = {fk.column for fk in context.foreign_keys_by_table.get(table_key, [])}
    pk_columns = set(context.pk_columns(table_key))
    measures: list[str] = []
    for column in context.columns_by_table.get(table_key, []):
        if column.name in fk_columns or column.name in pk_columns:
            continue
        if is_numeric_column(column):
            measures.append(column.name)
    return measures


def first_date_column(columns: list[ColumnInfo]) -> str | None:
    date_columns = [column.name for column in columns if is_date_column(column)]
    if not date_columns:
        return None
    return sorted(date_columns, key=date_preference)[0]


def date_preference(column_name: str) -> tuple[int, str]:
    name = column_name.lower()
    if "effective" in name:
        return (0, name)
    if "completed" in name:
        return (1, name)
    if "started" in name or name.startswith("start"):
        return (2, name)
    if "opened" in name or "created" in name:
        return (3, name)
    return (5, name)


def short_table_name(schema: str, table: str) -> str:
    return f"{schema}.{table}" if schema else table


def name_tokens(name: str) -> set[str]:
    tokens: set[str] = set()
    for token in re.findall(r"[A-Za-z0-9]+", name.lower()):
        if token:
            tokens.add(token)
    for part in name.lower().split("_"):
        if part:
            tokens.add(part)
    return tokens


def is_free_text_column(column: ColumnInfo) -> bool:
    tokens = name_tokens(column.name)
    if tokens.intersection(FREE_TEXT_TOKENS):
        return True
    data_type = column.data_type.lower()
    return "text" in data_type and any(token in column.name.lower() for token in FREE_TEXT_TOKENS)


def is_domain_column(column: ColumnInfo) -> bool:
    tokens = name_tokens(column.name)
    return bool(tokens.intersection(DOMAIN_TOKENS))


def is_compact_text_attribute_column(column: ColumnInfo) -> bool:
    if is_free_text_column(column):
        return False
    data_type = column.data_type.lower()
    if not any(token in data_type for token in ("char", "clob", "text", "uuid", "varchar")):
        return False
    return bool(name_tokens(column.name).intersection(COMPACT_TEXT_ATTRIBUTE_TOKENS))


def is_dynamic_label_column(column: ColumnInfo) -> bool:
    if is_free_text_column(column) or is_domain_column(column):
        return False
    data_type = column.data_type.lower()
    if not any(token in data_type for token in ("char", "clob", "text", "uuid", "varchar")):
        return False
    tokens = name_tokens(column.name)
    return bool(tokens.intersection(COMPACT_IDENTIFIER_TOKENS))


def dynamic_label_preference(column_name: str) -> int:
    tokens = name_tokens(column_name)
    for index, token in enumerate(("number", "code", "identifier", "reference", "ref")):
        if token in tokens:
            return index
    return 99


def is_date_column(column: ColumnInfo) -> bool:
    data_type = column.data_type.lower()
    tokens = name_tokens(column.name)
    return bool(tokens.intersection(DATE_TOKENS)) or any(token in data_type for token in ("date", "time"))


def is_numeric_column(column: ColumnInfo) -> bool:
    data_type = column.data_type.lower()
    if column.name.lower().endswith("_id") or name_tokens(column.name).intersection(ID_TOKENS):
        return False
    return any(token in data_type for token in ("int", "real", "floa", "doub", "num", "dec"))


def numeric_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def render_attributes(attrs: dict[str, Any]) -> str:
    return "; ".join(f"{key}={format_value(value)}" for key, value in attrs.items())


def render_mapping(mapping: dict[str, Any]) -> str:
    return "; ".join(f"{key}={format_number(value) if isinstance(value, (int, float)) else format_value(value)}" for key, value in mapping.items())


def format_number(value: int | float) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def format_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return format_number(value)
    text = str(value)
    if not text:
        return "<empty>"
    text = truncate_value(text)
    if re.search(r"\s", text):
        return repr(text)
    return text


def truncate_value(value: Any, limit: int = MAX_TEXT_VALUE_LENGTH) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "..."


def flatten_for_search(value: Any) -> str:
    flattened: list[str] = []

    def walk(current: Any) -> None:
        if len(flattened) >= 600:
            return
        if hasattr(current, "__dataclass_fields__"):
            walk(asdict(current))
        elif isinstance(current, dict):
            for key, child in current.items():
                flattened.append(str(key))
                walk(child)
        elif isinstance(current, list):
            for child in current:
                walk(child)
        elif current is not None:
            flattened.append(str(current))

    walk(value)
    return " ".join(flattened[:600])
