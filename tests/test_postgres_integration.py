from __future__ import annotations

import os
import uuid

import pytest

from contextty.models import SnapshotOptions
from contextty.services import inspect_source, query_context, refresh_snapshot
from contextty.storage import LocalStore


@pytest.mark.integration
def test_live_postgres_inspect_snapshot_and_query(tmp_path, monkeypatch) -> None:
    dsn = os.environ.get("CONTEXTTY_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("CONTEXTTY_TEST_DATABASE_URL is not set")
    try:
        import psycopg
    except ModuleNotFoundError:
        pytest.skip("psycopg is not installed")

    schema = f"contextty_test_{uuid.uuid4().hex[:8]}"
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA "{schema}"')
                cur.execute(
                    f'''
                    CREATE TABLE "{schema}".users (
                        id integer PRIMARY KEY,
                        email text NOT NULL,
                        signup_state text,
                        created_at timestamp with time zone NOT NULL DEFAULT now()
                    )
                    '''
                )
                cur.execute(
                    f'''
                    CREATE TABLE "{schema}".orders (
                        id integer PRIMARY KEY,
                        user_id integer NOT NULL REFERENCES "{schema}".users(id),
                        total_cents integer NOT NULL
                    )
                    '''
                )
                cur.execute(f'CREATE INDEX orders_user_id_idx ON "{schema}".orders(user_id)')
                cur.execute(
                    f'''
                    INSERT INTO "{schema}".users(id, email, signup_state)
                    VALUES (1, 'a@example.com', 'verified'), (2, 'b@example.com', 'pending')
                    '''
                )
                cur.execute(f'INSERT INTO "{schema}".orders(id, user_id, total_cents) VALUES (1, 1, 500)')
    except Exception as exc:
        pytest.skip(f"could not prepare live Postgres fixture: {exc}")

    monkeypatch.setenv("DATABASE_URL", dsn)
    store = LocalStore(tmp_path / "contextty.db")
    store.add_source("app-db", "postgres", "DATABASE_URL")

    try:
        inspection = inspect_source(store, "app-db")
        assert any(table["schema"] == schema and table["name"] == "users" for table in inspection["tables"])

        snapshot = refresh_snapshot(
            store,
            "app-db",
            SnapshotOptions(profile_mode="deep", row_limit=100, timeout_seconds=5),
        )
        assert snapshot["nodes"] > 0
        assert "signup_state" in query_context(store, "signup state", source_name="app-db")["context"]
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
