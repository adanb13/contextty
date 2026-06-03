from __future__ import annotations

import sqlite3

from contextty.graph import ContextGraph
from contextty.models import SnapshotOptions
from contextty.services import query_context, refresh_snapshot
from contextty.storage import LocalStore, dumps_json

from .helpers import populated_store, sqlite_fixture_db


def test_source_storage_uses_connector_specific_locator_fields(tmp_path) -> None:
    sqlite_path = sqlite_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")

    postgres = store.add_source("pg-db", "postgres", dsn_env="DATABASE_URL")
    sqlite = store.add_source("local-db", "sqlite", path=sqlite_path)
    mysql = store.add_source("mysql-db", "mysql", dsn_env="MYSQL_DATABASE_URL")
    mariadb = store.add_source("mariadb-db", "mariadb", dsn_env="MARIADB_DATABASE_URL")
    duckdb = store.add_source("duckdb-db", "duckdb", path=tmp_path / "analytics.duckdb")

    assert postgres.dsn_env == "DATABASE_URL"
    assert postgres.path is None
    assert sqlite.dsn_env is None
    assert sqlite.path and sqlite.path.endswith("app.sqlite3")
    assert mysql.dsn_env == "MYSQL_DATABASE_URL"
    assert mysql.path is None
    assert mariadb.dsn_env == "MARIADB_DATABASE_URL"
    assert mariadb.path is None
    assert duckdb.dsn_env is None
    assert duckdb.path and duckdb.path.endswith("analytics.duckdb")

    with store.connect() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(sources)").fetchall()}
    assert {"connector_type", "dsn_env", "path"} <= columns


def test_existing_store_migrates_source_connector_check(tmp_path) -> None:
    store_path = tmp_path / "contextty.db"
    with sqlite3.connect(store_path) as conn:
        conn.executescript(
            """
            CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                connector_type TEXT NOT NULL CHECK (connector_type IN ('postgres', 'sqlite')),
                dsn_env TEXT,
                path TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            INSERT INTO sources(name, connector_type, dsn_env, path)
            VALUES ('pg-db', 'postgres', 'DATABASE_URL', NULL);
            """
        )

    store = LocalStore(store_path)
    store.add_source("mysql-db", "mysql", dsn_env="MYSQL_DATABASE_URL")
    store.add_source("mariadb-db", "mariadb", dsn_env="MARIADB_DATABASE_URL")
    store.add_source("duckdb-db", "duckdb", path=tmp_path / "analytics.duckdb")

    assert {source.connector_type for source in store.list_sources()} == {"postgres", "mysql", "mariadb", "duckdb"}


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
    assert any(pill["kind"] == "table_inventory" and "row_counts" in pill["rendered_text"] for pill in pills)
    assert any(pill["kind"] == "table_schema" and "primary_key=(id)" in pill["rendered_text"] for pill in pills)
    assert any(pill["kind"] == "value_domain" and "signup_state" in pill["rendered_text"] for pill in pills)
    assert any(pill["kind"] == "column_group" and "public.orders.user_id" in pill["rendered_text"] for pill in pills)
    assert any(pill["kind"] == "relationship_card" and "public.orders" in pill["rendered_text"] for pill in pills)


def test_query_context_and_find_path_use_local_graph(tmp_path) -> None:
    store, source = populated_store(tmp_path)
    graph = ContextGraph(store, source_id=source.id)

    result = graph.query_context("what tables explain signup state?", budget=2000)
    assert "signup_state" in result["context"]
    assert "value_domain public.users.signup_state" in result["context"]
    assert result["facts"]
    assert result["routing_hints"]["likely_columns"][0]["qualified_name"].endswith("signup_state")
    assert any(node["kind"] == "table" and node["name"] == "users" for node in result["nodes"])

    nodes_by_name = {node["qualified_name"]: node["id"] for node in store.get_nodes(source.id)}
    path = graph.find_path(nodes_by_name["app.public.orders.user_id"], nodes_by_name["app.public.users.id"])
    assert [node["qualified_name"] for node in path["path"]] == [
        "app.public.orders.user_id",
        "app.public.users.id",
    ]


def test_query_context_uses_compact_facts_for_schema_profile_questions(tmp_path) -> None:
    store, source = populated_store(tmp_path)
    graph = ContextGraph(store, source_id=source.id)

    result = graph.query_context("signup state values and counts", budget=50000)

    assert result["answerability"]["status"] == "answered_by_snapshot"
    assert "verified=7" in result["context"]
    assert "pending=2" in result["context"]
    assert result["pills"] == []
    assert len(dumps_json(result)) < 15000


def test_query_context_marks_missing_row_questions_as_db_fallback(tmp_path) -> None:
    store, source = populated_store(tmp_path)
    graph = ContextGraph(store, source_id=source.id)

    result = graph.query_context("Who is Zelda Quinn's manager?", budget=50000)

    assert result["answerability"]["status"] == "needs_db_fallback"
    assert result["context"].startswith("NEEDS_DB_FALLBACK")
    assert result["facts"] == []
    assert result["pills"] == []


def test_deep_sqlite_snapshot_row_facts_answer_bounded_row_questions(tmp_path) -> None:
    db_path = hr_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")
    store.add_source("hr-db", "sqlite", path=db_path)

    snapshot = refresh_snapshot(store, "hr-db", SnapshotOptions(profile_mode="deep", row_limit=100, timeout_seconds=5))

    assert snapshot["facts"] > snapshot["pills"]
    facts = store.get_facts(kind="latest_metric")
    assert any("Samir Patel" in fact["text"] and "154000" in fact["text"] for fact in facts)
    row_facts = [fact for fact in store.get_facts() if fact["kind"] in {"aggregate", "bridge", "entity", "latest_metric", "relationship"}]
    assert not any("exceeded delivery" in fact["text"].lower() for fact in row_facts)

    manager = query_context(store, "Who is Priya Nair's manager?", source_name="hr-db")
    assert manager["answerability"]["status"] == "answered_by_snapshot"
    assert "Luis Martinez" in manager["context"]

    project = query_context(
        store,
        "Which employees are assigned to Manager Insights Dashboard, with their role and allocation_percent?",
        source_name="hr-db",
    )
    assert "Priya Nair" in project["context"]
    assert "Aisha Johnson" in project["context"]
    assert "data_partner" in project["context"]
    assert "60" in project["context"]

    owner = query_context(
        store,
        "Which department owns Payroll Modernization, and what is that department's cost_center?",
        source_name="hr-db",
    )
    assert "Finance" in owner["context"]
    assert "FIN-500" in owner["context"]

    salary = query_context(store, "What is the average latest salary for Engineering employees?", source_name="hr-db")
    assert "Engineering=155500" in salary["context"]

    reviews = query_context(store, "What is the average 2025-H2 performance review rating by department?", source_name="hr-db")
    assert "Engineering=4.25" in reviews["context"]
    assert "Product=4" in reviews["context"]


def test_report_render_failure_preserves_successful_snapshot(tmp_path, monkeypatch) -> None:
    db_path = sqlite_fixture_db(tmp_path)
    store = LocalStore(tmp_path / "contextty.db")
    source = store.add_source("local-db", "sqlite", path=db_path)

    def fail_report(*_args, **_kwargs) -> None:
        raise RuntimeError("report renderer failed")

    monkeypatch.setattr("contextty.services.write_snapshot_report", fail_report)

    snapshot = refresh_snapshot(store, "local-db", SnapshotOptions(row_limit=100, timeout_seconds=5))

    assert snapshot["report_error"] == "report renderer failed"
    assert "report_path" not in snapshot
    run = store.latest_snapshot_run(source.id)
    assert run is not None
    assert run.status == "success"
    assert store.get_nodes(source.id)


def test_fact_index_search_uses_local_facts(tmp_path) -> None:
    store, source = populated_store(tmp_path)

    facts = store.search_facts("signup state verified counts", source_id=source.id)

    assert facts
    assert facts[0]["kind"] == "value_domain"
    assert "verified=7" in facts[0]["text"]


def hr_fixture_db(tmp_path):
    path = tmp_path / "hr.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, cost_center TEXT NOT NULL UNIQUE)")
        conn.execute(
            """
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                department_id INTEGER NOT NULL REFERENCES departments(id),
                manager_id INTEGER REFERENCES employees(id),
                employment_status TEXT NOT NULL,
                hire_date TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                department_id INTEGER NOT NULL REFERENCES departments(id),
                status TEXT NOT NULL,
                started_on TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE employee_projects (
                employee_id INTEGER NOT NULL REFERENCES employees(id),
                project_id INTEGER NOT NULL REFERENCES projects(id),
                role TEXT NOT NULL,
                allocation_percent INTEGER NOT NULL,
                PRIMARY KEY (employee_id, project_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE salary_history (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL REFERENCES employees(id),
                effective_date TEXT NOT NULL,
                salary INTEGER NOT NULL,
                currency TEXT NOT NULL,
                reason TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE performance_reviews (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL REFERENCES employees(id),
                reviewer_id INTEGER NOT NULL REFERENCES employees(id),
                review_period TEXT NOT NULL,
                rating INTEGER NOT NULL,
                summary TEXT NOT NULL,
                completed_on TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO departments(id, name, cost_center) VALUES (?, ?, ?)",
            [(1, "Engineering", "ENG-100"), (2, "Product", "PRD-200"), (5, "Finance", "FIN-500")],
        )
        conn.executemany(
            "INSERT INTO employees(id, first_name, last_name, department_id, manager_id, employment_status, hire_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, "Maya", "Chen", 2, None, "active", "2019-01-07"),
                (2, "Luis", "Martinez", 1, 1, "active", "2020-03-16"),
                (3, "Priya", "Nair", 1, 2, "active", "2021-06-21"),
                (4, "Owen", "Brooks", 1, 2, "active", "2022-10-03"),
                (5, "Aisha", "Johnson", 2, 1, "active", "2021-04-12"),
                (8, "Marcus", "Reed", 5, 1, "active", "2023-02-13"),
                (10, "Samir", "Patel", 1, 2, "active", "2023-07-17"),
            ],
        )
        conn.executemany(
            "INSERT INTO projects(id, name, department_id, status, started_on) VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Payroll Modernization", 5, "active", "2025-10-01"),
                (2, "Employee Self Service Portal", 1, "active", "2025-11-15"),
                (5, "Manager Insights Dashboard", 2, "active", "2025-12-01"),
            ],
        )
        conn.executemany(
            "INSERT INTO employee_projects(employee_id, project_id, role, allocation_percent) VALUES (?, ?, ?, ?)",
            [(3, 5, "data_partner", 25), (5, 5, "product_owner", 60), (8, 1, "finance_owner", 50)],
        )
        conn.executemany(
            "INSERT INTO salary_history(id, employee_id, effective_date, salary, currency, reason) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1, 2, "2025-01-01", 188000, "USD", "annual_adjustment"),
                (2, 3, "2025-01-01", 158000, "USD", "annual_adjustment"),
                (3, 4, "2025-01-01", 122000, "USD", "annual_adjustment"),
                (4, 10, "2025-01-01", 146000, "USD", "annual_adjustment"),
                (5, 10, "2026-01-01", 154000, "USD", "promotion"),
            ],
        )
        conn.executemany(
            "INSERT INTO performance_reviews(id, employee_id, reviewer_id, review_period, rating, summary, completed_on) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, 2, 1, "2025-H2", 5, "Exceeded delivery and retention goals.", "2026-01-12"),
                (2, 3, 2, "2025-H2", 4, "Strong technical leadership.", "2026-01-14"),
                (3, 4, 2, "2025-H2", 3, "Delivered committed scope.", "2026-01-15"),
                (4, 5, 1, "2025-H2", 4, "Improved roadmap clarity.", "2026-01-13"),
                (5, 10, 2, "2025-H2", 5, "Led migration work.", "2026-01-15"),
            ],
        )
    return path
