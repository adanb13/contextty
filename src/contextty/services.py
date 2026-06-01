from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .connectors.postgres import PostgresConnector, PostgresIntrospector
from .detect import detect_project
from .graph import ContextGraph
from .models import InspectionResult, SnapshotOptions
from .snapshot import build_artifact
from .storage import LocalStore


def detect(path: str = ".") -> dict[str, Any]:
    return detect_project(path)


def add_source(
    store: LocalStore,
    name: str,
    connector_type: str,
    dsn_env: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return asdict(store.add_source(name, connector_type, dsn_env, metadata))


def list_sources(store: LocalStore) -> list[dict[str, Any]]:
    return [asdict(source) for source in store.list_sources()]


def inspect_source(
    store: LocalStore,
    source_name: str,
    timeout_seconds: float = 5.0,
    introspector: PostgresIntrospector | None = None,
) -> dict[str, Any]:
    source = store.get_source(source_name)
    if source.connector_type != "postgres":
        raise ValueError("v0.0.1 only supports Postgres sources")
    introspector = introspector or PostgresIntrospector()
    connector = PostgresConnector.from_env(source.dsn_env, timeout_seconds=timeout_seconds)
    with connector.connect() as conn:
        inspection = introspector.inspect(conn)
    return inspection_to_dict(inspection)


def refresh_snapshot(
    store: LocalStore,
    source_name: str,
    options: SnapshotOptions,
    inspection: InspectionResult | None = None,
    profiles: dict[tuple[str, str], Any] | None = None,
    introspector: PostgresIntrospector | None = None,
) -> dict[str, Any]:
    source = store.get_source(source_name)
    run = store.create_snapshot_run(
        source_id=source.id,
        profile_mode=options.profile_mode,
        row_limit=options.row_limit,
        timeout_seconds=options.timeout_seconds,
    )
    try:
        if inspection is None:
            introspector = introspector or PostgresIntrospector()
            connector = PostgresConnector.from_env(source.dsn_env, timeout_seconds=options.timeout_seconds)
            with connector.connect() as conn:
                inspection = introspector.inspect(conn)
                profiles = introspector.profile(conn, inspection, options)
        nodes, edges, pills = build_artifact(source, run, inspection, profiles)
        store.replace_artifact(source.id, run.id, nodes, edges, pills)
        run = store.finish_snapshot_run(
            run.id,
            "success",
            metadata={
                "nodes": len(nodes),
                "edges": len(edges),
                "pills": len(pills),
                "database": inspection.database,
            },
        )
        return {
            "run": asdict(run),
            "nodes": len(nodes),
            "edges": len(edges),
            "pills": len(pills),
        }
    except Exception as exc:
        store.finish_snapshot_run(run.id, "failed", error=str(exc))
        raise


def query_context(
    store: LocalStore,
    query: str,
    budget: int = 2000,
    source_name: str | None = None,
    hops: int = 2,
    direction: str = "both",
) -> dict[str, Any]:
    source_id = store.get_source(source_name).id if source_name else None
    graph = ContextGraph(store, source_id=source_id)
    return graph.query_context(query=query, budget=budget, hops=hops, direction=direction)


def get_node(store: LocalStore, node_id: str) -> dict[str, Any] | None:
    node = store.get_node(node_id)
    if not node:
        return None
    graph = ContextGraph(store, source_id=node["source_id"], snapshot_run_id=node["snapshot_run_id"])
    return graph.get_node(node_id)


def get_neighbors(store: LocalStore, node_id: str, hops: int = 1, direction: str = "both") -> dict[str, Any]:
    node = store.get_node(node_id)
    if not node:
        return {"nodes": [], "edges": []}
    graph = ContextGraph(store, source_id=node["source_id"], snapshot_run_id=node["snapshot_run_id"])
    return graph.get_neighbors(node_id, hops=hops, direction=direction)


def find_path(store: LocalStore, start_node_id: str, end_node_id: str, direction: str = "both") -> dict[str, Any]:
    start = store.get_node(start_node_id)
    if not start:
        return {"path": [], "edges": []}
    graph = ContextGraph(store, source_id=start["source_id"], snapshot_run_id=start["snapshot_run_id"])
    return graph.find_path(start_node_id, end_node_id, direction=direction)


def graph_summary(store: LocalStore, source_name: str | None = None) -> dict[str, Any]:
    source_id = store.get_source(source_name).id if source_name else None
    return ContextGraph(store, source_id=source_id).graph_summary()


def inspection_to_dict(inspection: InspectionResult) -> dict[str, Any]:
    return {
        "database": inspection.database,
        "tables": [asdict(table) for table in inspection.tables],
        "columns": [asdict(column) for column in inspection.columns],
        "primary_keys": [asdict(pk) for pk in inspection.primary_keys],
        "foreign_keys": [asdict(fk) for fk in inspection.foreign_keys],
        "indexes": [asdict(index) for index in inspection.indexes],
        "views": [asdict(view) for view in inspection.views],
    }
