from __future__ import annotations

import json
import threading
from dataclasses import asdict
from typing import Any

import click

from .api import create_app
from .connectors.postgres import parse_timeout
from .mcp_server import MCPServer
from .models import SnapshotOptions
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
    """Detect likely Postgres source configuration in a project."""
    _json(detect(path))


@main.group("source")
def source_group() -> None:
    """Manage registered sources."""


@source_group.command("add")
@click.argument("name")
@click.option("--type", "connector_type", required=True, type=click.Choice(["postgres"]), help="Connector type.")
@click.option("--dsn-env", required=True, help="Environment variable containing the Postgres DSN.")
def source_add_cmd(name: str, connector_type: str, dsn_env: str) -> None:
    """Register or update a source."""
    try:
        _json(add_source(_store(), name, connector_type, dsn_env))
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
    """Inspect a registered Postgres source without writing a snapshot."""
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


if __name__ == "__main__":
    main()
