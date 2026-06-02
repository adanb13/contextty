from __future__ import annotations

import json
import sys
import threading
from dataclasses import asdict
from typing import Any

import click

from .api import create_app
from .connectors.common import parse_timeout
from .mcp_server import MCPServer
from .models import CONNECTOR_TYPES, SnapshotOptions
from .services import (
    add_source,
    detect,
    graph_summary,
    inspect_source,
    list_sources,
    query_context,
    refresh_snapshot,
)
from .storage import LocalStore


def _store() -> LocalStore:
    return LocalStore()


def _json(data: Any) -> None:
    click.echo(json.dumps(data, indent=2, sort_keys=True, default=str))


@click.group()
def main() -> None:
    """Build local AI-readable context artifacts from approved data sources."""


@main.command("detect")
@click.argument("path", default=".", required=False, type=click.Path(exists=True))
def detect_cmd(path: str) -> None:
    """Detect likely database source configuration in a project."""
    result = detect(path)
    if not _detect_interactive():
        _json(result)
        return

    registered: list[dict[str, Any]] = []
    sources = result.get("sources", [])
    if not sources:
        click.echo("No database sources detected.")
        _json({"detection": result, "registered": registered})
        return

    store = _store()
    for candidate in sources:
        label = _candidate_label(candidate)
        if not click.confirm(f"Register {label}?", default=False):
            continue
        name = click.prompt("Source name", default=candidate.get("name") or "source")
        metadata = {"detected_from": candidate.get("source"), "confidence": candidate.get("confidence")}
        registered.append(
            add_source(
                store,
                name,
                candidate["connector_type"],
                dsn_env=candidate.get("dsn_env"),
                path=candidate.get("path"),
                metadata=metadata,
            )
        )
    _json({"detection": result, "registered": registered})


@main.group("source")
def source_group() -> None:
    """Manage registered sources."""


@source_group.command("add")
@click.argument("name")
@click.option("--type", "connector_type", required=True, type=click.Choice(CONNECTOR_TYPES), help="Connector type.")
@click.option("--dsn-env", default=None, help="Environment variable containing a network database DSN.")
@click.option("--path", "source_path", default=None, type=click.Path(), help="Local database path.")
def source_add_cmd(name: str, connector_type: str, dsn_env: str | None, source_path: str | None) -> None:
    """Register or update a source."""
    try:
        _json(add_source(_store(), name, connector_type, dsn_env=dsn_env, path=source_path))
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@source_group.command("list")
def source_list_cmd() -> None:
    """List registered sources."""
    _json(list_sources(_store()))


@main.command("inspect")
@click.argument("source_name")
@click.option("--timeout", default="5s", show_default=True, help="Statement timeout, e.g. 500ms, 5s, 1m.")
def inspect_cmd(source_name: str, timeout: str) -> None:
    """Inspect a registered source without writing a snapshot."""
    try:
        _json(inspect_source(_store(), source_name, timeout_seconds=parse_timeout(timeout)))
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("snapshot")
@click.argument("source_name")
@click.option("--profile-mode", default="basic", show_default=True, type=click.Choice(["basic", "deep"]))
@click.option("--row-limit", default=1000, show_default=True, type=int)
@click.option("--timeout", default="5s", show_default=True, help="Statement timeout, e.g. 500ms, 5s, 1m.")
def snapshot_cmd(source_name: str, profile_mode: str, row_limit: int, timeout: str) -> None:
    """Refresh the local graph and context pills for a source."""
    try:
        result = refresh_snapshot(
            _store(),
            source_name,
            SnapshotOptions(
                profile_mode=profile_mode,
                row_limit=row_limit,
                timeout_seconds=parse_timeout(timeout),
            ),
        )
        _json(result)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("query")
@click.argument("question")
@click.option("--budget", default=2000, show_default=True, type=int)
@click.option("--source", "source_name", default=None)
@click.option("--hops", default=2, show_default=True, type=int)
@click.option("--direction", default="both", show_default=True, type=click.Choice(["both", "out", "in", "reverse"]))
def query_cmd(question: str, budget: int, source_name: str | None, hops: int, direction: str) -> None:
    """Query the local artifact only."""
    try:
        _json(query_context(_store(), question, budget=budget, source_name=source_name, hops=hops, direction=direction))
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("graph")
@click.option("--source", "source_name", default=None)
def graph_cmd(source_name: str | None) -> None:
    """Return the latest local graph."""
    try:
        _json(graph_summary(_store(), source_name=source_name))
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("serve")
@click.option("--api/--no-api", default=False, help="Serve the HTTP API.")
@click.option("--mcp/--no-mcp", default=False, help="Serve MCP over stdio.")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8765, show_default=True, type=int)
def serve_cmd(api: bool, mcp: bool, host: str, port: int) -> None:
    """Serve the HTTP API, MCP stdio server, or both."""
    if not api and not mcp:
        api = True

    if api and mcp:
        thread = threading.Thread(target=_run_uvicorn, args=(host, port), daemon=True)
        thread.start()
        MCPServer(_store()).serve_stdio()
        return

    if api:
        _run_uvicorn(host, port)
        return

    MCPServer(_store()).serve_stdio()


def _run_uvicorn(host: str, port: int) -> None:
    import uvicorn

    uvicorn.run(create_app(_store()), host=host, port=port)


def _detect_interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _candidate_label(candidate: dict[str, Any]) -> str:
    if candidate["connector_type"] == "postgres":
        return f"Postgres source from {candidate.get('dsn_env')}"
    if candidate["connector_type"] == "mysql":
        return f"MySQL source from {candidate.get('dsn_env')}"
    if candidate["connector_type"] == "mariadb":
        return f"MariaDB source from {candidate.get('dsn_env')}"
    if candidate["connector_type"] == "sqlite":
        return f"SQLite source at {candidate.get('path')}"
    if candidate["connector_type"] == "duckdb":
        return f"DuckDB source at {candidate.get('path')}"
    return f"{candidate['connector_type']} source"


if __name__ == "__main__":
    main()
