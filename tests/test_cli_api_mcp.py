from __future__ import annotations

import json

from click.testing import CliRunner
from fastapi.testclient import TestClient

from contextty.api import create_app
from contextty.cli import main
from contextty.mcp_server import MCPServer
from contextty.storage import LocalStore

from .helpers import populated_store


def test_cli_source_add_list_and_query(tmp_path) -> None:
    db_path = tmp_path / "contextty.db"
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["source", "add", "app-db", "--type", "postgres", "--dsn-env", "DATABASE_URL"],
        env={"CONTEXTTY_STORE_PATH": str(db_path)},
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["name"] == "app-db"

    store = LocalStore(db_path)
    populated_store_path = tmp_path / "populated.db"
    populated, _source = populated_store(tmp_path / "populated")
    populated.path.replace(populated_store_path)

    result = runner.invoke(
        main,
        ["query", "signup state", "--source", "app-db"],
        env={"CONTEXTTY_STORE_PATH": str(populated_store_path)},
    )
    assert result.exit_code == 0, result.output
    assert "signup_state" in json.loads(result.output)["context"]


def test_api_sources_query_graph_and_node(tmp_path) -> None:
    store, source = populated_store(tmp_path)
    client = TestClient(create_app(store))

    assert client.get("/v1/sources").json()[0]["name"] == "app-db"

    query = client.post("/v1/query", json={"query": "orders by user", "source": "app-db"})
    assert query.status_code == 200
    assert "orders" in query.json()["context"]

    graph = client.get("/v1/graph", params={"source": "app-db"})
    assert graph.status_code == 200
    table_node = next(node for node in graph.json()["nodes"] if node["qualified_name"] == "app.public.users")

    node = client.get(f"/v1/nodes/{table_node['id']}")
    assert node.status_code == 200
    assert node.json()["name"] == "users"


def test_mcp_tool_listing_and_query_context(tmp_path) -> None:
    store, _source = populated_store(tmp_path)
    server = MCPServer(store)

    tool_names = {tool["name"] for tool in server.list_tools()}
    assert tool_names >= {"list_sources", "inspect_source", "refresh_snapshot", "query_context", "get_node", "get_neighbors", "find_path"}
    assert "execute_sql" not in tool_names

    result = server.call_tool("query_context", {"query": "signup state", "source": "app-db"})
    assert "signup_state" in result["context"]

    rpc = server.handle_jsonrpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert rpc and rpc["result"]["tools"]
