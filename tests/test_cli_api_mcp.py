from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner
from fastapi.testclient import TestClient

from contextty.api import SnapshotRequest, create_app
from contextty.cli import main
from contextty.detect import detect_project
from contextty.mcp_server import MCPServer
from contextty.models import SnapshotOptions
from contextty.storage import LocalStore

from .helpers import populated_store, sqlite_fixture_db


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


def test_cli_sqlite_source_add_inspect_snapshot_and_query(tmp_path) -> None:
    db_path = sqlite_fixture_db(tmp_path)
    store_path = tmp_path / "contextty.db"
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["source", "add", "local-db", "--type", "sqlite", "--path", str(db_path)],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    source = json.loads(result.output)
    assert source["connector_type"] == "sqlite"
    assert source["path"].endswith("app.sqlite3")
    assert source["dsn_env"] is None

    result = runner.invoke(
        main,
        ["inspect", "local-db"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    assert any(table["name"] == "users" for table in json.loads(result.output)["tables"])

    result = runner.invoke(
        main,
        ["snapshot", "local-db", "--profile-mode", "deep", "--row-limit", "100"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["nodes"] > 0

    result = runner.invoke(
        main,
        ["query", "signup state", "--source", "local-db"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    assert "signup_state" in json.loads(result.output)["context"]


def test_snapshot_defaults_are_deep_and_basic_can_be_explicit(tmp_path) -> None:
    assert SnapshotOptions().profile_mode == "deep"
    assert SnapshotOptions(profile_mode="basic").profile_mode == "basic"
    assert SnapshotRequest(source="local-db").profile_mode == "deep"

    cli_dir = tmp_path / "cli"
    cli_dir.mkdir()
    cli_db_path = sqlite_fixture_db(cli_dir)
    cli_store_path = cli_dir / ".contextty" / "contextty.db"
    runner = CliRunner()
    env = {"CONTEXTTY_STORE_PATH": str(cli_store_path)}

    result = runner.invoke(
        main,
        ["source", "add", "local-db", "--type", "sqlite", "--path", str(cli_db_path)],
        env=env,
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(main, ["snapshot", "local-db", "--row-limit", "100"], env=env)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["run"]["profile_mode"] == "deep"
    result = runner.invoke(main, ["snapshot", "local-db", "--profile-mode", "basic", "--row-limit", "100"], env=env)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["run"]["profile_mode"] == "basic"

    api_dir = tmp_path / "api"
    api_dir.mkdir()
    api_db_path = sqlite_fixture_db(api_dir)
    client = TestClient(create_app(LocalStore(api_dir / "contextty.db")))
    created = client.post("/v1/sources", json={"name": "local-db", "type": "sqlite", "path": str(api_db_path)})
    assert created.status_code == 200, created.text
    snapshot = client.post("/v1/snapshot", json={"source": "local-db", "row_limit": 100})
    assert snapshot.status_code == 200, snapshot.text
    assert snapshot.json()["run"]["profile_mode"] == "deep"
    snapshot = client.post("/v1/snapshot", json={"source": "local-db", "profile_mode": "basic", "row_limit": 100})
    assert snapshot.status_code == 200, snapshot.text
    assert snapshot.json()["run"]["profile_mode"] == "basic"

    mcp_dir = tmp_path / "mcp"
    mcp_dir.mkdir()
    mcp_db_path = sqlite_fixture_db(mcp_dir)
    server = MCPServer(LocalStore(mcp_dir / ".contextty" / "contextty.db"))
    refresh_tool = next(tool for tool in server.list_tools() if tool["name"] == "refresh_snapshot")
    assert refresh_tool["inputSchema"]["properties"]["profile_mode"]["default"] == "deep"
    server.call_tool("add_source", {"name": "local-db", "type": "sqlite", "path": str(mcp_db_path)})
    snapshot = server.call_tool("refresh_snapshot", {"source": "local-db", "row_limit": 100})
    assert snapshot["run"]["profile_mode"] == "deep"
    snapshot = server.call_tool("refresh_snapshot", {"source": "local-db", "profile_mode": "basic", "row_limit": 100})
    assert snapshot["run"]["profile_mode"] == "basic"


def test_cli_snapshot_writes_report_and_artifact_commands_accept_store_paths(tmp_path) -> None:
    db_path = sqlite_fixture_db(tmp_path)
    contextty_dir = tmp_path / ".contextty"
    store_path = contextty_dir / "contextty.db"
    runner = CliRunner()
    env = {"CONTEXTTY_STORE_PATH": str(store_path)}

    result = runner.invoke(
        main,
        ["source", "add", "local-db", "--type", "sqlite", "--path", str(db_path)],
        env=env,
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(main, ["snapshot", "local-db", "--row-limit", "100"], env=env)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run"]["profile_mode"] == "deep"
    assert payload["report_path"].endswith(".contextty/reports/local-db.html")
    report_path = Path(payload["report_path"])
    assert report_path.exists()

    html = report_path.read_text(encoding="utf-8")
    assert "local-db" in html
    assert f"Run #{payload['run']['id']}" in html
    assert 'id="contextty-report-data"' in html
    assert 'id="graph-map"' in html
    assert "signup_state" in html
    assert "foreign_key_to" in html
    assert "value_domain" in html

    for contextty_path in (None, str(contextty_dir), str(store_path)):
        args = ["list"] if contextty_path is None else ["list", contextty_path]
        result = runner.invoke(main, args, env=env)
        assert result.exit_code == 0, result.output
        artifacts = json.loads(result.output)
        assert artifacts[0]["name"] == "local-db"
        assert artifacts[0]["run_id"] == payload["run"]["id"]
        assert artifacts[0]["report_path"] == str(report_path)

    report_path.unlink()
    result = runner.invoke(main, ["create-report", "local-db", str(contextty_dir)], env=env)
    assert result.exit_code == 0, result.output
    regenerated = json.loads(result.output)
    assert regenerated["name"] == "local-db"
    assert regenerated["report_path"] == str(report_path)
    assert report_path.exists()


def test_cli_detect_noninteractive_outputs_json_without_registering(tmp_path, monkeypatch) -> None:
    for name in ("DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN", "MYSQL_URL", "MYSQL_DATABASE_URL", "MYSQL_DSN", "MARIADB_URL", "MARIADB_DATABASE_URL", "MARIADB_DSN"):
        monkeypatch.delenv(name, raising=False)
    sqlite_fixture_db(tmp_path)
    (tmp_path / "not-a-database.db").write_text("not sqlite", encoding="utf-8")
    store_path = tmp_path / "contextty.db"

    result = CliRunner().invoke(
        main,
        ["detect", str(tmp_path)],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert [source["connector_type"] for source in payload["sources"]] == ["sqlite"]
    assert payload["sources"][0]["path"].endswith("app.sqlite3")
    assert LocalStore(store_path).list_sources() == []


def test_cli_detect_interactive_can_register_sqlite_source(tmp_path, monkeypatch) -> None:
    for name in ("DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN", "MYSQL_URL", "MYSQL_DATABASE_URL", "MYSQL_DSN", "MARIADB_URL", "MARIADB_DATABASE_URL", "MARIADB_DSN"):
        monkeypatch.delenv(name, raising=False)
    sqlite_fixture_db(tmp_path)
    store_path = tmp_path / "contextty.db"
    monkeypatch.setattr("contextty.cli._detect_interactive", lambda: True)

    result = CliRunner().invoke(
        main,
        ["detect", str(tmp_path)],
        input="y\nlocal-db\n",
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )

    assert result.exit_code == 0, result.output
    source = LocalStore(store_path).get_source("local-db")
    assert source.connector_type == "sqlite"
    assert source.path and source.path.endswith("app.sqlite3")


def test_detect_mysql_and_mariadb_env_names_and_url_schemes(tmp_path, monkeypatch) -> None:
    for name in ("POSTGRES_URL", "POSTGRES_DSN", "PG_DSN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATABASE_URL", "mysql+pymysql://user:pass@localhost/app")
    monkeypatch.setenv("MYSQL_DATABASE_URL", "mysql://user:pass@localhost/app")
    monkeypatch.setenv("MARIADB_DATABASE_URL", "mariadb://user:pass@localhost/app")
    (tmp_path / ".env").write_text(
        "REPORTING_URL=mariadb+pymysql://user:pass@localhost/reporting\nMYSQL_DSN=\n",
        encoding="utf-8",
    )

    sources = detect_project(tmp_path)["sources"]
    by_env = {source["dsn_env"]: source["connector_type"] for source in sources if source.get("dsn_env")}

    assert by_env["DATABASE_URL"] == "mysql"
    assert by_env["MYSQL_DATABASE_URL"] == "mysql"
    assert by_env["MARIADB_DATABASE_URL"] == "mariadb"
    assert by_env["REPORTING_URL"] == "mariadb"
    assert by_env["MYSQL_DSN"] == "mysql"


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


def test_api_sqlite_source_create_snapshot_and_query(tmp_path) -> None:
    db_path = sqlite_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")
    client = TestClient(create_app(store))

    created = client.post("/v1/sources", json={"name": "local-db", "type": "sqlite", "path": str(db_path)})
    assert created.status_code == 200, created.text
    assert created.json()["connector_type"] == "sqlite"
    assert created.json()["dsn_env"] is None

    inspected = client.post("/v1/inspect", json={"source": "local-db"})
    assert inspected.status_code == 200, inspected.text
    assert any(table["name"] == "verified_users" and table["kind"] == "view" for table in inspected.json()["tables"])

    snapshot = client.post(
        "/v1/snapshot",
        json={"source": "local-db", "profile_mode": "deep", "row_limit": 100},
    )
    assert snapshot.status_code == 200, snapshot.text
    assert snapshot.json()["nodes"] > 0

    query = client.post("/v1/query", json={"query": "signup state", "source": "local-db"})
    assert query.status_code == 200, query.text
    assert "signup_state" in query.json()["context"]


def test_cli_api_mcp_register_mysql_and_mariadb_sources(tmp_path) -> None:
    store_path = tmp_path / "contextty.db"
    runner = CliRunner()

    mysql = runner.invoke(
        main,
        ["source", "add", "mysql-db", "--type", "mysql", "--dsn-env", "MYSQL_DATABASE_URL"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert mysql.exit_code == 0, mysql.output
    assert json.loads(mysql.output)["connector_type"] == "mysql"

    mariadb = runner.invoke(
        main,
        ["source", "add", "mariadb-db", "--type", "mariadb", "--dsn-env", "MARIADB_DATABASE_URL"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert mariadb.exit_code == 0, mariadb.output
    assert json.loads(mariadb.output)["connector_type"] == "mariadb"

    client = TestClient(create_app(LocalStore(tmp_path / "api.db")))
    created = client.post("/v1/sources", json={"name": "mysql-db", "type": "mysql", "dsn_env": "MYSQL_DATABASE_URL"})
    assert created.status_code == 200, created.text
    assert created.json()["dsn_env"] == "MYSQL_DATABASE_URL"

    server = MCPServer(LocalStore(tmp_path / "mcp.db"))
    assert server.call_tool("add_source", {"name": "mariadb-db", "type": "mariadb", "dsn_env": "MARIADB_DATABASE_URL"})[
        "connector_type"
    ] == "mariadb"


def test_mcp_tool_listing_and_query_context(tmp_path) -> None:
    store, _source = populated_store(tmp_path)
    server = MCPServer(store)

    tool_names = {tool["name"] for tool in server.list_tools()}
    assert tool_names >= {
        "detect_sources",
        "add_source",
        "list_sources",
        "inspect_source",
        "refresh_snapshot",
        "query_context",
        "get_node",
        "get_neighbors",
        "find_path",
    }
    assert "execute_sql" not in tool_names

    result = server.call_tool("query_context", {"query": "signup state", "source": "app-db"})
    assert "signup_state" in result["context"]

    rpc = server.handle_jsonrpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert rpc and rpc["result"]["tools"]
    add_source_tool = next(tool for tool in rpc["result"]["tools"] if tool["name"] == "add_source")
    assert set(add_source_tool["inputSchema"]["properties"]["type"]["enum"]) >= {"mysql", "mariadb", "duckdb"}


def test_mcp_sqlite_detect_add_snapshot_and_query(tmp_path, monkeypatch) -> None:
    for name in ("DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN", "MYSQL_URL", "MYSQL_DATABASE_URL", "MYSQL_DSN", "MARIADB_URL", "MARIADB_DATABASE_URL", "MARIADB_DSN"):
        monkeypatch.delenv(name, raising=False)
    db_path = sqlite_fixture_db(tmp_path)
    server = MCPServer(LocalStore(tmp_path / ".contextty" / "contextty.db"))

    detected = server.call_tool("detect_sources", {"path": str(tmp_path)})
    assert [source["connector_type"] for source in detected["sources"]] == ["sqlite"]

    created = server.call_tool("add_source", {"name": "local-db", "type": "sqlite", "path": str(db_path)})
    assert created["connector_type"] == "sqlite"
    assert created["path"].endswith("app.sqlite3")

    snapshot = server.call_tool("refresh_snapshot", {"source": "local-db", "profile_mode": "deep", "row_limit": 100})
    assert snapshot["nodes"] > 0
    assert "signup_state" in server.call_tool("query_context", {"query": "signup state", "source": "local-db"})["context"]
