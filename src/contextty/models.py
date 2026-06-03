from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

CONNECTOR_TYPES = ("postgres", "sqlite", "mysql", "mariadb", "duckdb")
DSN_ENV_CONNECTOR_TYPES = ("postgres", "mysql", "mariadb")
PATH_CONNECTOR_TYPES = ("sqlite", "duckdb")

ConnectorType = Literal["postgres", "sqlite", "mysql", "mariadb", "duckdb"]
ProfileMode = Literal["basic", "deep"]


@dataclass(slots=True)
class Source:
    id: int
    name: str
    connector_type: ConnectorType
    dsn_env: str | None
    path: str | None
    created_at: str
    updated_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SnapshotRun:
    id: int
    source_id: int
    started_at: str
    finished_at: str | None
    profile_mode: ProfileMode
    row_limit: int
    timeout_seconds: float
    status: str
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TableInfo:
    database: str
    schema: str
    name: str
    kind: str = "table"
    row_estimate: int | None = None
    size_bytes: int | None = None
    comment: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"


@dataclass(slots=True)
class ColumnInfo:
    database: str
    schema: str
    table: str
    name: str
    ordinal: int
    data_type: str
    nullable: bool
    default: str | None = None
    character_max_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    comment: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}.{self.name}"


@dataclass(slots=True)
class PrimaryKeyInfo:
    database: str
    schema: str
    table: str
    column: str
    ordinal: int
    constraint_name: str


@dataclass(slots=True)
class ForeignKeyInfo:
    database: str
    schema: str
    table: str
    column: str
    ref_schema: str
    ref_table: str
    ref_column: str
    constraint_name: str


@dataclass(slots=True)
class IndexInfo:
    database: str
    schema: str
    table: str
    name: str
    columns: list[str] = field(default_factory=list)
    unique: bool = False
    primary: bool = False
    definition: str | None = None


@dataclass(slots=True)
class ViewInfo:
    database: str
    schema: str
    name: str
    definition: str | None = None


@dataclass(slots=True)
class InspectionResult:
    database: str
    tables: list[TableInfo] = field(default_factory=list)
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_keys: list[PrimaryKeyInfo] = field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    views: list[ViewInfo] = field(default_factory=list)


@dataclass(slots=True)
class ColumnProfile:
    null_count: int | None = None
    null_rate: float | None = None
    distinct_count: int | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    top_values: list[dict[str, Any]] = field(default_factory=list)
    patterns: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class TableProfile:
    sample_count: int | None = None
    row_count: int | None = None
    row_count_is_capped: bool = False
    time_windows: list[dict[str, Any]] = field(default_factory=list)
    columns: dict[str, ColumnProfile] = field(default_factory=dict)


@dataclass(slots=True)
class SnapshotOptions:
    profile_mode: ProfileMode = "deep"
    row_limit: int = 1000
    timeout_seconds: float = 5.0
    time_window: str = "day"
