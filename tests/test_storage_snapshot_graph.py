from __future__ import annotations

from contextty.graph import ContextGraph
from contextty.storage import LocalStore

from .helpers import populated_store, sqlite_fixture_db


def test_source_storage_uses_connector_specific_locator_fields(tmp_path) -> None:
    sqlite_path = sqlite_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")

    postgres = store.add_source("pg-db", "postgres", dsn_env="DATABASE_URL")
    sqlite = store.add_source("local-db", "sqlite", path=sqlite_path)

    assert postgres.dsn_env == "DATABASE_URL"
    assert postgres.path is None
    assert sqlite.dsn_env is None
    assert sqlite.path and sqlite.path.endswith("app.sqlite3")

    with store.connect() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(sources)").fetchall()}
    assert {"connector_type", "dsn_env", "path"} <= columns


def test_snapshot_artifact_contains_expected_nodes_edges_and_pills(tmp_path) -> None:
    store, source = populated_store(tmp_path)

    nodes = store.get_nodes(source.id)
    edges = store.get_edges(source.id)
    pills = store.get_pills(source.id)

    assert {node["kind"] for node in nodes} >= {"database", "schema", "table", "column", "index", "context_pill"}
    assert any(node["qualified_name"] == "app.public.users" for node in nodes)
    assert any(edge["relation"] == "foreign_key_to" for edge in edges)
    assert any(pill["kind"] == "text_patterns" for pill in pills)
    assert any("signup_state" in pill["rendered_text"] for pill in pills)


def test_query_context_and_find_path_use_local_graph(tmp_path) -> None:
    store, source = populated_store(tmp_path)
    graph = ContextGraph(store, source_id=source.id)

    result = graph.query_context("what tables explain signup state?", budget=2000)
    assert "signup_state" in result["context"]
    assert any(node["kind"] == "table" and node["name"] == "users" for node in result["nodes"])

    nodes_by_name = {node["qualified_name"]: node["id"] for node in store.get_nodes(source.id)}
    path = graph.find_path(nodes_by_name["app.public.orders.user_id"], nodes_by_name["app.public.users.id"])
    assert [node["qualified_name"] for node in path["path"]] == [
        "app.public.orders.user_id",
        "app.public.users.id",
    ]
