from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

duckdb = pytest.importorskip("duckdb")

from contextty.api import create_app
from contextty.cli import main
from contextty.connectors.duckdb import DuckDBConnector, DuckDBIntrospector
from contextty.detect import detect_project
from contextty.mcp_server import MCPServer
from contextty.models import SnapshotOptions
from contextty.services import query_context, refresh_snapshot
from contextty.storage import LocalStore
from contextty.safety import UnsafeSQLError


def test_duckdb_introspection_tables_views_keys_and_indexes(tmp_path) -> None:
    db_path = duckdb_fixture_db(tmp_path)

    with DuckDBConnector(db_path).connect() as conn:
        inspection = DuckDBIntrospector().inspect(conn)

    assert {(table.schema, table.name, table.kind) for table in inspection.tables} >= {
        ("main", "users", "table"),
        ("main", "orders", "table"),
        ("main", "verified_users", "view"),
    }
    assert {(column.table, column.name, column.nullable) for column in inspection.columns} >= {
        ("users", "id", False),
        ("users", "email", False),
        ("orders", "user_id", False),
    }
    assert {(pk.table, pk.column, pk.ordinal) for pk in inspection.primary_keys} >= {
        ("users", "id", 1),
        ("orders", "id", 1),
    }
    assert any(index.name == "orders_user_id_idx" and index.columns == ["user_id"] for index in inspection.indexes)
    assert any(view.name == "verified_users" and "users" in (view.definition or "") for view in inspection.views)


def test_duckdb_connector_and_query_helper_are_readonly(tmp_path) -> None:
    db_path = duckdb_fixture_db(tmp_path)

    with DuckDBConnector(db_path).connect() as conn:
        rows = DuckDBIntrospector().execute_readonly_query(
            conn,
            "select signup_state from users where id = 1",
        )
        assert rows == [{"signup_state": "verified"}]

        with pytest.raises(UnsafeSQLError):
            DuckDBIntrospector().execute_readonly_query(conn, "drop table users")

        with pytest.raises(Exception):
            conn.execute("INSERT INTO users(id, email, created_at) VALUES (99, 'x@example.com', '2026-01-01')")


def test_duckdb_deep_profile_and_row_facts(tmp_path) -> None:
    db_path = duckdb_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")
    store.add_source("analytics-db", "duckdb", path=db_path)

    snapshot = refresh_snapshot(
        store,
        "analytics-db",
        SnapshotOptions(profile_mode="deep", row_limit=100, timeout_seconds=5.0),
    )

    assert snapshot["nodes"] > 0
    facts = store.get_facts(kind="entity")
    assert facts
    assert any("signup_state=verified" in fact["text"] for fact in facts)

    result = query_context(store, "signup state", source_name="analytics-db")
    assert "signup_state" in result["context"]

    with DuckDBConnector(db_path).connect() as conn:
        introspector = DuckDBIntrospector()
        inspection = introspector.inspect(conn)
        profiles = introspector.profile(
            conn,
            inspection,
            SnapshotOptions(profile_mode="deep", row_limit=100, timeout_seconds=5.0),
        )

    users = profiles[("main", "users")]
    assert users.row_count == 3
    assert users.columns["signup_state"].null_count == 1
    assert users.columns["signup_state"].null_rate == pytest.approx(1 / 3)
    assert {"value": "verified", "count": 1} in users.columns["signup_state"].top_values
    assert users.time_windows
    assert str(users.time_windows[0]["window_start"]).startswith("2026-01-01")


def test_duckdb_detection_only_accepts_duckdb_suffixes(tmp_path) -> None:
    duckdb_fixture_db(tmp_path, "analytics.duckdb")
    duckdb_fixture_db(tmp_path, "not-detected.db")

    detected = detect_project(tmp_path)

    duckdb_sources = [source for source in detected["sources"] if source["connector_type"] == "duckdb"]
    assert len(duckdb_sources) == 1
    assert duckdb_sources[0]["path"].endswith("analytics.duckdb")


def test_cli_duckdb_source_add_inspect_snapshot_and_query(tmp_path) -> None:
    db_path = duckdb_fixture_db(tmp_path)
    store_path = tmp_path / "contextty.db"
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["source", "add", "analytics-db", "--type", "duckdb", "--path", str(db_path)],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    source = json.loads(result.output)
    assert source["connector_type"] == "duckdb"
    assert source["path"].endswith("analytics.duckdb")
    assert source["dsn_env"] is None

    result = runner.invoke(
        main,
        ["inspect", "analytics-db"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    assert any(table["name"] == "users" for table in json.loads(result.output)["tables"])

    result = runner.invoke(
        main,
        ["snapshot", "analytics-db", "--profile-mode", "deep", "--row-limit", "100"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["nodes"] > 0

    result = runner.invoke(
        main,
        ["query", "signup state", "--source", "analytics-db"],
        env={"CONTEXTTY_STORE_PATH": str(store_path)},
    )
    assert result.exit_code == 0, result.output
    assert "signup_state" in json.loads(result.output)["context"]


def test_api_duckdb_source_create_snapshot_and_query(tmp_path) -> None:
    db_path = duckdb_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")
    client = TestClient(create_app(store))

    created = client.post("/v1/sources", json={"name": "analytics-db", "type": "duckdb", "path": str(db_path)})
    assert created.status_code == 200, created.text
    assert created.json()["connector_type"] == "duckdb"

    inspected = client.post("/v1/inspect", json={"source": "analytics-db"})
    assert inspected.status_code == 200, inspected.text
    assert any(table["name"] == "verified_users" and table["kind"] == "view" for table in inspected.json()["tables"])

    snapshot = client.post(
        "/v1/snapshot",
        json={"source": "analytics-db", "profile_mode": "deep", "row_limit": 100},
    )
    assert snapshot.status_code == 200, snapshot.text
    assert snapshot.json()["nodes"] > 0

    query = client.post("/v1/query", json={"query": "signup state", "source": "analytics-db"})
    assert query.status_code == 200, query.text
    assert "signup_state" in query.json()["context"]


def test_mcp_duckdb_detect_add_snapshot_and_query(tmp_path, monkeypatch) -> None:
    for name in ("DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN", "MYSQL_URL", "MARIADB_URL"):
        monkeypatch.delenv(name, raising=False)
    db_path = duckdb_fixture_db(tmp_path)
    server = MCPServer(LocalStore(tmp_path / ".contextty" / "contextty.db"))

    detected = server.call_tool("detect_sources", {"path": str(tmp_path)})
    assert [source["connector_type"] for source in detected["sources"]] == ["duckdb"]

    created = server.call_tool("add_source", {"name": "analytics-db", "type": "duckdb", "path": str(db_path)})
    assert created["connector_type"] == "duckdb"
    assert created["path"].endswith("analytics.duckdb")

    snapshot = server.call_tool("refresh_snapshot", {"source": "analytics-db", "profile_mode": "deep", "row_limit": 100})
    assert snapshot["nodes"] > 0
    assert "signup_state" in server.call_tool("query_context", {"query": "signup state", "source": "analytics-db"})["context"]


def duckdb_fixture_db(tmp_path, filename: str = "analytics.duckdb") -> Path:
    path = Path(tmp_path) / filename
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email VARCHAR NOT NULL UNIQUE,
                signup_state VARCHAR,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                total_cents INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX orders_user_id_idx ON orders(user_id)")
        conn.execute(
            """
            CREATE VIEW verified_users AS
            SELECT id, email
            FROM users
            WHERE signup_state = 'verified'
            """
        )
        conn.executemany(
            "INSERT INTO users(id, email, signup_state, created_at) VALUES (?, ?, ?, ?)",
            [
                (1, "a@example.com", "verified", "2026-01-01T00:00:00"),
                (2, "b@example.com", "pending", "2026-01-02T00:00:00"),
                (3, "c@example.com", None, "2026-01-02T02:00:00"),
            ],
        )
        conn.executemany(
            "INSERT INTO orders(id, user_id, total_cents) VALUES (?, ?, ?)",
            [(1, 1, 500), (2, 1, 750), (3, 2, 1250)],
        )
    finally:
        conn.close()
    return path
