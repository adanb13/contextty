from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .connectors.registry import connector_for_source
from .detect import detect_project
from .facts import facts_from_pills
from .graph import ContextGraph
from .models import InspectionResult, SnapshotOptions, Source
from .reports import report_path_for_source, write_snapshot_report
from .snapshot import build_artifact
from .storage import LocalStore


def detect(path: str = ".") -> dict[str, Any]:
    return detect_project(path)


def add_source(
    store: LocalStore,
    name: str,
    connector_type: str,
    dsn_env: str | None = None,
    path: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return asdict(store.add_source(name, connector_type, dsn_env=dsn_env, path=path, metadata=metadata))


def list_sources(store: LocalStore) -> list[dict[str, Any]]:
    return [asdict(source) for source in store.list_sources()]


def inspect_source(
    store: LocalStore,
    source_name: str,
    timeout_seconds: float = 5.0,
    introspector: Any | None = None,
) -> dict[str, Any]:
    source = store.get_source(source_name)
    inspection, _profiles = _inspect_and_profile(
        source,
        SnapshotOptions(profile_mode="basic", timeout_seconds=timeout_seconds),
        introspector=introspector,
        include_profiles=False,
    )
    return inspection_to_dict(inspection)


def refresh_snapshot(
    store: LocalStore,
    source_name: str,
    options: SnapshotOptions,
    inspection: InspectionResult | None = None,
    profiles: dict[tuple[str, str], Any] | None = None,
    introspector: Any | None = None,
) -> dict[str, Any]:
    source = store.get_source(source_name)
    run = store.create_snapshot_run(
        source_id=source.id,
        profile_mode=options.profile_mode,
        row_limit=options.row_limit,
        timeout_seconds=options.timeout_seconds,
    )
    try:
        row_facts: list[dict[str, Any]] = []
        if inspection is None:
            inspection, profiles, row_facts = _inspect_profile_and_facts(
                source,
                options,
                run,
                introspector=introspector,
                include_profiles=True,
            )
        nodes, edges, pills = build_artifact(source, run, inspection, profiles)
        facts = facts_from_pills(pills) + row_facts
        store.replace_artifact(source.id, run.id, nodes, edges, pills, facts=facts)
        run = store.finish_snapshot_run(
            run.id,
            "success",
            metadata={
                "nodes": len(nodes),
                "edges": len(edges),
                "pills": len(pills),
                "facts": len(facts),
                "database": inspection.database,
            },
        )
        result = {
            "run": asdict(run),
            "nodes": len(nodes),
            "edges": len(edges),
            "pills": len(pills),
            "facts": len(facts),
        }
        try:
            result["report_path"] = str(write_snapshot_report(store, source.name, snapshot_run_id=run.id))
        except Exception as report_exc:
            result["report_error"] = str(report_exc)
        return result
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


def list_artifacts(store: LocalStore) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for source in store.list_sources():
        run = store.latest_snapshot_run(source.id)
        if run is None:
            continue
        metadata = run.metadata or {}
        report_path = report_path_for_source(store, source.name)
        artifacts.append(
            {
                "name": source.name,
                "source_name": source.name,
                "source_id": source.id,
                "connector_type": source.connector_type,
                "run_id": run.id,
                "profile_mode": run.profile_mode,
                "row_limit": run.row_limit,
                "timeout_seconds": run.timeout_seconds,
                "finished_at": run.finished_at,
                "database": metadata.get("database"),
                "nodes": metadata.get("nodes", 0),
                "edges": metadata.get("edges", 0),
                "pills": metadata.get("pills", 0),
                "facts": metadata.get("facts", 0),
                "report_path": str(report_path) if report_path.exists() else None,
            }
        )
    return artifacts


def create_report(store: LocalStore, artifact_name: str) -> dict[str, Any]:
    source = store.get_source(artifact_name)
    run = store.latest_snapshot_run(source.id)
    if run is None:
        raise KeyError(f"successful snapshot artifact not found: {artifact_name}")
    report_path = write_snapshot_report(store, source.name, snapshot_run_id=run.id)
    return {
        "name": source.name,
        "source_name": source.name,
        "run": asdict(run),
        "report_path": str(report_path),
    }


def _inspect_and_profile(
    source: Source,
    options: SnapshotOptions,
    introspector: Any | None = None,
    include_profiles: bool = True,
) -> tuple[InspectionResult, dict[tuple[str, str], Any] | None]:
    inspection, profiles, _facts = _inspect_profile_and_facts(
        source,
        options,
        run=None,
        introspector=introspector,
        include_profiles=include_profiles,
    )
    return inspection, profiles


def _inspect_profile_and_facts(
    source: Source,
    options: SnapshotOptions,
    run: Any | None,
    introspector: Any | None = None,
    include_profiles: bool = True,
) -> tuple[InspectionResult, dict[tuple[str, str], Any] | None, list[dict[str, Any]]]:
    connector, active_introspector = connector_for_source(source, options, introspector=introspector)

    with connector.connect() as conn:
        inspection = active_introspector.inspect(conn)
        profiles = active_introspector.profile(conn, inspection, options) if include_profiles else None
        row_facts = (
            active_introspector.derive_facts(conn, source, run, inspection, options)
            if run is not None and include_profiles and hasattr(active_introspector, "derive_facts")
            else []
        )
    return inspection, profiles, row_facts


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
