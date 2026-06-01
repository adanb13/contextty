from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .connectors.common import parse_timeout
from .models import SnapshotOptions
from .services import (
    add_source,
    detect,
    get_neighbors,
    get_node,
    graph_summary,
    inspect_source,
    list_sources,
    query_context,
    refresh_snapshot,
)
from .storage import LocalStore


class SourceCreate(BaseModel):
    name: str
    type: str = "postgres"
    dsn_env: str | None = None
    path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class InspectRequest(BaseModel):
    source: str
    timeout: str | float = "5s"


class SnapshotRequest(BaseModel):
    source: str
    profile_mode: str = "basic"
    row_limit: int = 1000
    timeout: str | float = "5s"
    time_window: str = "day"


class QueryRequest(BaseModel):
    query: str
    budget: int = 2000
    source: str | None = None
    hops: int = 2
    direction: str = "both"


def create_app(store: LocalStore | None = None) -> FastAPI:
    app = FastAPI(title="Contextty", version="0.0.1")
    store = store or LocalStore()

    @app.get("/v1/sources")
    def sources() -> list[dict[str, Any]]:
        return list_sources(store)

    @app.get("/v1/detect")
    def detect_sources(path: str = ".") -> dict[str, Any]:
        return detect(path)

    @app.post("/v1/sources")
    def create_source(request: SourceCreate) -> dict[str, Any]:
        try:
            return add_source(
                store,
                request.name,
                request.type,
                dsn_env=request.dsn_env,
                path=request.path,
                metadata=request.metadata,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/inspect")
    def inspect(request: InspectRequest) -> dict[str, Any]:
        try:
            return inspect_source(store, request.source, timeout_seconds=parse_timeout(request.timeout))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/snapshot")
    def snapshot(request: SnapshotRequest) -> dict[str, Any]:
        try:
            return refresh_snapshot(
                store,
                request.source,
                SnapshotOptions(
                    profile_mode=request.profile_mode,  # type: ignore[arg-type]
                    row_limit=request.row_limit,
                    timeout_seconds=parse_timeout(request.timeout),
                    time_window=request.time_window,
                ),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/query")
    def query(request: QueryRequest) -> dict[str, Any]:
        try:
            return query_context(
                store,
                request.query,
                budget=request.budget,
                source_name=request.source,
                hops=request.hops,
                direction=request.direction,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/graph")
    def graph(source: str | None = None) -> dict[str, Any]:
        try:
            return graph_summary(store, source_name=source)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/v1/nodes/{node_id:path}/neighbors")
    def neighbors(node_id: str, hops: int = 1, direction: str = "both") -> dict[str, Any]:
        return get_neighbors(store, node_id, hops=hops, direction=direction)

    @app.get("/v1/nodes/{node_id:path}")
    def node(node_id: str) -> dict[str, Any]:
        result = get_node(store, node_id)
        if result is None:
            raise HTTPException(status_code=404, detail="node not found")
        return result

    return app
