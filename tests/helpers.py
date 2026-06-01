from __future__ import annotations

import sqlite3
from pathlib import Path

from contextty.models import (
    ColumnInfo,
    ColumnProfile,
    ForeignKeyInfo,
    IndexInfo,
    InspectionResult,
    PrimaryKeyInfo,
    Source,
    TableInfo,
    TableProfile,
)
from contextty.snapshot import build_artifact
from contextty.storage import LocalStore


def sample_inspection() -> InspectionResult:
    return InspectionResult(
        database="app",
        tables=[
            TableInfo(database="app", schema="public", name="users", row_estimate=10, size_bytes=8192),
            TableInfo(database="app", schema="public", name="orders", row_estimate=25, size_bytes=16384),
        ],
        columns=[
            ColumnInfo(database="app", schema="public", table="users", name="id", ordinal=1, data_type="integer", nullable=False),
            ColumnInfo(database="app", schema="public", table="users", name="email", ordinal=2, data_type="text", nullable=False),
            ColumnInfo(database="app", schema="public", table="users", name="signup_state", ordinal=3, data_type="text", nullable=True),
            ColumnInfo(database="app", schema="public", table="users", name="created_at", ordinal=4, data_type="timestamp with time zone", nullable=False),
            ColumnInfo(database="app", schema="public", table="orders", name="id", ordinal=1, data_type="integer", nullable=False),
            ColumnInfo(database="app", schema="public", table="orders", name="user_id", ordinal=2, data_type="integer", nullable=False),
            ColumnInfo(database="app", schema="public", table="orders", name="total_cents", ordinal=3, data_type="integer", nullable=False),
        ],
        primary_keys=[
            PrimaryKeyInfo(database="app", schema="public", table="users", column="id", ordinal=1, constraint_name="users_pkey"),
            PrimaryKeyInfo(database="app", schema="public", table="orders", column="id", ordinal=1, constraint_name="orders_pkey"),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                database="app",
                schema="public",
                table="orders",
                column="user_id",
                ref_schema="public",
                ref_table="users",
                ref_column="id",
                constraint_name="orders_user_id_fkey",
            )
        ],
        indexes=[
            IndexInfo(database="app", schema="public", table="users", name="users_pkey", columns=["id"], unique=True, primary=True),
            IndexInfo(database="app", schema="public", table="users", name="users_email_idx", columns=["email"], unique=True),
            IndexInfo(database="app", schema="public", table="orders", name="orders_user_id_idx", columns=["user_id"]),
        ],
    )


def sample_profiles() -> dict[tuple[str, str], TableProfile]:
    return {
        ("public", "users"): TableProfile(
            sample_count=10,
            row_count=10,
            time_windows=[
                {"window_start": "2026-01-01T00:00:00Z", "count": 4},
                {"window_start": "2026-01-02T00:00:00Z", "count": 6},
            ],
            columns={
                "id": ColumnProfile(null_count=0, null_rate=0, distinct_count=10, min_value=1, max_value=10),
                "email": ColumnProfile(
                    null_count=0,
                    null_rate=0,
                    distinct_count=10,
                    top_values=[{"value": "a@example.com", "count": 1}],
                    patterns=[{"template": "user <num> @ example . com", "count": 5}],
                ),
                "signup_state": ColumnProfile(
                    null_count=1,
                    null_rate=0.1,
                    distinct_count=2,
                    top_values=[{"value": "verified", "count": 7}, {"value": "pending", "count": 2}],
                    patterns=[{"template": "verified", "count": 7}, {"template": "pending", "count": 2}],
                ),
                "created_at": ColumnProfile(null_count=0, null_rate=0, distinct_count=10),
            },
        ),
        ("public", "orders"): TableProfile(
            sample_count=25,
            row_count=25,
            columns={
                "id": ColumnProfile(null_count=0, null_rate=0, distinct_count=25, min_value=1, max_value=25),
                "user_id": ColumnProfile(null_count=0, null_rate=0, distinct_count=10, min_value=1, max_value=10),
                "total_cents": ColumnProfile(null_count=0, null_rate=0, distinct_count=20, min_value=100, max_value=10000),
            },
        ),
    }


def populated_store(tmp_path) -> tuple[LocalStore, Source]:
    store = LocalStore(tmp_path / "contextty.db")
    source = store.add_source("app-db", "postgres", "DATABASE_URL")
    run = store.create_snapshot_run(source.id, "deep", 1000, 5.0)
    nodes, edges, pills = build_artifact(source, run, sample_inspection(), sample_profiles())
    store.replace_artifact(source.id, run.id, nodes, edges, pills)
    store.finish_snapshot_run(run.id, "success", metadata={"nodes": len(nodes), "edges": len(edges), "pills": len(pills)})
    return store, source


def sqlite_fixture_db(tmp_path) -> Path:
    path = Path(tmp_path) / "app.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                signup_state TEXT,
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
    return path
