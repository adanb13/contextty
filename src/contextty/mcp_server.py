from __future__ import annotations

import json
import sys
from typing import Any, Callable

from .connectors.common import parse_timeout
from .models import SnapshotOptions
from .services import (
    add_source,
    detect,
    get_neighbors,
    get_node,
    inspect_source,
    list_sources,
    query_context,
    refresh_snapshot,
    find_path as find_path_service,
)
from .storage import LocalStore


class MCPServer:
    def __init__(self, store: LocalStore | None = None) -> None:
        self.store = store or LocalStore()
        self._handlers: dict[str, Callable[[dict[str, Any]], Any]] = {
            "detect_sources": self._detect_sources,
            "add_source": self._add_source,
            "list_sources": self._list_sources,
            "inspect_source": self._inspect_source,
            "refresh_snapshot": self._refresh_snapshot,
            "query_context": self._query_context,
            "get_node": self._get_node,
            "get_neighbors": self._get_neighbors,
            "find_path": self._find_path,
        }

    def list_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "detect_sources",
                "description": "Detect Postgres and SQLite sources under a project path.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "default": "."},
                    },
                    "additionalProperties": False,
                },
            },
            {
                "name": "add_source",
                "description": "Register or update a Contextty source using connector-specific fields.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string", "enum": ["postgres", "sqlite"]},
                        "dsn_env": {"type": "string"},
                        "path": {"type": "string"},
                        "metadata": {"type": "object", "default": {}},
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "list_sources",
                "description": "List registered Contextty sources.",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            },
            {
                "name": "inspect_source",
                "description": "Inspect live source schema through a read-only connection; use query_context for answering from snapshots.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "timeout": {"type": ["string", "number"], "default": "5s"},
                    },
                    "required": ["source"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "refresh_snapshot",
                "description": "Refresh a local snapshot for a registered source.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "profile_mode": {"type": "string", "enum": ["basic", "deep"], "default": "basic"},
                        "row_limit": {"type": "integer", "default": 1000},
                        "timeout": {"type": ["string", "number"], "default": "5s"},
                    },
                    "required": ["source"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "query_context",
                "description": "Answer from the local snapshot fact index first; does not execute SQL or contact the live source.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "budget": {"type": "integer", "default": 2000},
                        "source": {"type": "string"},
                        "hops": {"type": "integer", "default": 2},
                        "direction": {"type": "string", "enum": ["both", "out", "in", "reverse"], "default": "both"},
                    },
                    "required": ["query"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_node",
                "description": "Return a graph node and attached pills from the local artifact.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"node_id": {"type": "string"}},
                    "required": ["node_id"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_neighbors",
                "description": "Return k-hop graph neighbors from the local artifact.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string"},
                        "hops": {"type": "integer", "default": 1},
                        "direction": {"type": "string", "enum": ["both", "out", "in", "reverse"], "default": "both"},
                    },
                    "required": ["node_id"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "find_path",
                "description": "Find the shortest graph path between two local artifact nodes.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "start_node_id": {"type": "string"},
                        "end_node_id": {"type": "string"},
                        "direction": {"type": "string", "enum": ["both", "out", "in", "reverse"], "default": "both"},
                    },
                    "required": ["start_node_id", "end_node_id"],
                    "additionalProperties": False,
                },
            },
        ]

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        if name not in self._handlers:
            raise KeyError(f"unknown MCP tool: {name}")
        return self._handlers[name](arguments or {})

    def serve_stdio(self) -> None:
        for line in sys.stdin:
            if not line.strip():
                continue
            response = self.handle_jsonrpc(json.loads(line))
            if response is not None:
                sys.stdout.write(json.dumps(response, default=str) + "\n")
                sys.stdout.flush()

    def handle_jsonrpc(self, request: dict[str, Any]) -> dict[str, Any] | None:
        request_id = request.get("id")
        method = request.get("method")
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "contextty", "version": "0.0.1"},
                    "capabilities": {"tools": {}},
                }
            elif method == "tools/list":
                result = {"tools": self.list_tools()}
            elif method == "tools/call":
                params = request.get("params") or {}
                result = self._tool_result(self.call_tool(params["name"], params.get("arguments") or {}))
            elif method and method.startswith("notifications/"):
                return None
            else:
                raise KeyError(f"unsupported method: {method}")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(exc)},
            }

    @staticmethod
    def _tool_result(payload: Any) -> dict[str, Any]:
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(payload, indent=2, sort_keys=True, default=str),
                }
            ]
        }

    def _list_sources(self, _arguments: dict[str, Any]) -> Any:
        return list_sources(self.store)

    def _detect_sources(self, arguments: dict[str, Any]) -> Any:
        return detect(arguments.get("path", "."))

    def _add_source(self, arguments: dict[str, Any]) -> Any:
        return add_source(
            self.store,
            arguments["name"],
            arguments["type"],
            dsn_env=arguments.get("dsn_env"),
            path=arguments.get("path"),
            metadata=arguments.get("metadata") or {},
        )

    def _inspect_source(self, arguments: dict[str, Any]) -> Any:
        return inspect_source(
            self.store,
            arguments["source"],
            timeout_seconds=parse_timeout(arguments.get("timeout", "5s")),
        )

    def _refresh_snapshot(self, arguments: dict[str, Any]) -> Any:
        return refresh_snapshot(
            self.store,
            arguments["source"],
            SnapshotOptions(
                profile_mode=arguments.get("profile_mode", "basic"),
                row_limit=int(arguments.get("row_limit", 1000)),
                timeout_seconds=parse_timeout(arguments.get("timeout", "5s")),
            ),
        )

    def _query_context(self, arguments: dict[str, Any]) -> Any:
        return query_context(
            self.store,
            arguments["query"],
            budget=int(arguments.get("budget", 2000)),
            source_name=arguments.get("source"),
            hops=int(arguments.get("hops", 2)),
            direction=arguments.get("direction", "both"),
        )

    def _get_node(self, arguments: dict[str, Any]) -> Any:
        return get_node(self.store, arguments["node_id"])

    def _get_neighbors(self, arguments: dict[str, Any]) -> Any:
        return get_neighbors(
            self.store,
            arguments["node_id"],
            hops=int(arguments.get("hops", 1)),
            direction=arguments.get("direction", "both"),
        )

    def _find_path(self, arguments: dict[str, Any]) -> Any:
        return find_path_service(
            self.store,
            arguments["start_node_id"],
            arguments["end_node_id"],
            direction=arguments.get("direction", "both"),
        )
