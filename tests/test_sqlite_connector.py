from __future__ import annotations

import sqlite3

import pytest

from contextty.connectors.sqlite import SQLiteConnector, SQLiteIntrospector
from contextty.models import SnapshotOptions
from contextty.safety import UnsafeSQLError

from .helpers import sqlite_fixture_db


def test_sqlite_introspection_tables_views_keys_and_indexes(tmp_path) -> None:
    db_path = sqlite_fixture_db(tmp_path)

    with SQLiteConnector(db_path).connect() as conn:
        inspection = SQLiteIntrospector().inspect(conn)

    assert inspection.database == "app.sqlite3"
    assert {(table.schema, table.name, table.kind) for table in inspection.tables} >= {
        ("main", "users", "table"),
        ("main", "orders", "table"),
        ("main", "verified_users", "view"),
    }
    assert {(column.table, column.name, column.data_type, column.nullable) for column in inspection.columns} >= {
        ("users", "id", "INTEGER", False),
        ("users", "email", "TEXT", False),
        ("orders", "user_id", "INTEGER", False),
    }
    assert {(pk.table, pk.column, pk.ordinal) for pk in inspection.primary_keys} >= {
        ("users", "id", 1),
        ("orders", "id", 1),
    }
    assert [(fk.table, fk.column, fk.ref_table, fk.ref_column) for fk in inspection.foreign_keys] == [
        ("orders", "user_id", "users", "id")
    ]
    assert any(index.name == "orders_user_id_idx" and index.columns == ["user_id"] for index in inspection.indexes)
    assert any(view.name == "verified_users" and "SELECT id, email" in (view.definition or "") for view in inspection.views)


def test_sqlite_connector_and_query_helper_are_readonly(tmp_path) -> None:
    db_path = sqlite_fixture_db(tmp_path)

    with SQLiteConnector(db_path).connect() as conn:
        rows = SQLiteIntrospector().execute_readonly_query(
            conn,
            "select signup_state from users where id = 1",
        )
        assert rows == [{"signup_state": "verified"}]

        with pytest.raises(UnsafeSQLError):
            SQLiteIntrospector().execute_readonly_query(conn, "drop table users")

        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO users(id, email, created_at) VALUES (99, 'x@example.com', '2026-01-01')")


def test_sqlite_deep_profile(tmp_path) -> None:
    db_path = sqlite_fixture_db(tmp_path)

    with SQLiteConnector(db_path).connect() as conn:
        introspector = SQLiteIntrospector()
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
    assert users.time_windows == [
        {"window_start": "2026-01-01", "count": 1},
        {"window_start": "2026-01-02", "count": 2},
    ]
