from __future__ import annotations

from contextty.graph import ContextGraph

from .helpers import populated_store


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
