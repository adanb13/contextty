from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .duckdb import DuckDBConnector, DuckDBIntrospector
from .mariadb import MariaDBConnector, MariaDBIntrospector
from .mysql import MySQLConnector, MySQLIntrospector
from .postgres import PostgresConnector, PostgresIntrospector
from .sqlite import SQLiteConnector, SQLiteIntrospector
from ..models import SnapshotOptions, Source


@dataclass(frozen=True, slots=True)
class ConnectorSpec:
    connector_type: str
    display_name: str
    locator: str
    connector_class: Any
    introspector_class: Any


CONNECTOR_REGISTRY: dict[str, ConnectorSpec] = {
    "postgres": ConnectorSpec("postgres", "Postgres", "dsn_env", PostgresConnector, PostgresIntrospector),
    "sqlite": ConnectorSpec("sqlite", "SQLite", "path", SQLiteConnector, SQLiteIntrospector),
    "mysql": ConnectorSpec("mysql", "MySQL", "dsn_env", MySQLConnector, MySQLIntrospector),
    "mariadb": ConnectorSpec("mariadb", "MariaDB", "dsn_env", MariaDBConnector, MariaDBIntrospector),
    "duckdb": ConnectorSpec("duckdb", "DuckDB", "path", DuckDBConnector, DuckDBIntrospector),
}


def connector_for_source(
    source: Source,
    options: SnapshotOptions,
    introspector: Any | None = None,
) -> tuple[Any, Any]:
    spec = CONNECTOR_REGISTRY.get(source.connector_type)
    if spec is None:
        raise ValueError(f"unsupported connector type: {source.connector_type}")

    active_introspector = introspector or _make_introspector(spec, options)
    if spec.locator == "dsn_env":
        if not source.dsn_env:
            raise ValueError(f"{spec.display_name} source {source.name} is missing dsn_env")
        connector = spec.connector_class.from_env(source.dsn_env, timeout_seconds=options.timeout_seconds)
    elif spec.locator == "path":
        if not source.path:
            raise ValueError(f"{spec.display_name} source {source.name} is missing path")
        connector = spec.connector_class(source.path, timeout_seconds=options.timeout_seconds)
    else:  # pragma: no cover - registry construction guard.
        raise ValueError(f"unsupported connector locator: {spec.locator}")
    return connector, active_introspector


def _make_introspector(spec: ConnectorSpec, options: SnapshotOptions) -> Any:
    try:
        return spec.introspector_class(timeout_seconds=options.timeout_seconds)
    except TypeError:
        return spec.introspector_class()
