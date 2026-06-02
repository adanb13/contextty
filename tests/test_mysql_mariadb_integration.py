from __future__ import annotations

import os
import uuid
from typing import Callable

import pytest

from contextty.connectors.mariadb import parse_mariadb_dsn
from contextty.connectors.mysql import parse_mysql_dsn
from contextty.models import SnapshotOptions
from contextty.services import inspect_source, query_context, refresh_snapshot
from contextty.storage import LocalStore


@pytest.mark.integration
@pytest.mark.parametrize(
    ("connector_type", "source_env", "test_env", "parser"),
    [
        ("mysql", "MYSQL_DATABASE_URL", "CONTEXTTY_MYSQL_TEST_DSN", parse_mysql_dsn),
        ("mariadb", "MARIADB_DATABASE_URL", "CONTEXTTY_MARIADB_TEST_DSN", parse_mariadb_dsn),
    ],
)
def test_live_mysql_compatible_inspect_snapshot_and_query(
    tmp_path,
    monkeypatch,
    connector_type: str,
    source_env: str,
    test_env: str,
    parser: Callable[[str], dict[str, object]],
) -> None:
    dsn = os.environ.get(test_env)
    if not dsn:
        pytest.skip(f"{test_env} is not set")
    try:
        import pymysql
        import pymysql.cursors
    except ModuleNotFoundError:
        pytest.skip("PyMySQL is not installed")

    suffix = uuid.uuid4().hex[:8]
    users_table = f"contextty_users_{suffix}"
    orders_table = f"contextty_orders_{suffix}"
    connect_args = parser(dsn)

    try:
        conn = pymysql.connect(
            **connect_args,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE `{users_table}` (
                        id INTEGER PRIMARY KEY,
                        email VARCHAR(255) NOT NULL,
                        signup_state VARCHAR(32),
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE `{orders_table}` (
                        id INTEGER PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        total_cents INTEGER NOT NULL,
                        CONSTRAINT `{orders_table}_user_fk` FOREIGN KEY (user_id) REFERENCES `{users_table}`(id)
                    )
                    """
                )
                cur.execute(f"CREATE INDEX `{orders_table}_user_id_idx` ON `{orders_table}`(user_id)")
                cur.execute(
                    f"""
                    INSERT INTO `{users_table}`(id, email, signup_state)
                    VALUES (1, 'a@example.com', 'verified'), (2, 'b@example.com', 'pending')
                    """
                )
                cur.execute(f"INSERT INTO `{orders_table}`(id, user_id, total_cents) VALUES (1, 1, 500)")
        finally:
            conn.close()
    except Exception as exc:
        pytest.skip(f"could not prepare live {connector_type} fixture: {exc}")

    monkeypatch.setenv(source_env, dsn)
    store = LocalStore(tmp_path / "contextty.db")
    store.add_source("app-db", connector_type, dsn_env=source_env)

    try:
        inspection = inspect_source(store, "app-db")
        assert any(table["name"] == users_table for table in inspection["tables"])

        snapshot = refresh_snapshot(
            store,
            "app-db",
            SnapshotOptions(profile_mode="deep", row_limit=100, timeout_seconds=5),
        )
        assert snapshot["nodes"] > 0
        assert "signup_state" in query_context(store, "signup state", source_name="app-db")["context"]
    finally:
        conn = pymysql.connect(**connect_args, autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{orders_table}`")
                cur.execute(f"DROP TABLE IF EXISTS `{users_table}`")
        finally:
            conn.close()
