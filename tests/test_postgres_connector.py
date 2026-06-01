from __future__ import annotations

import pytest

from contextty.connectors.postgres import (
    PostgresIntrospector,
    parse_index_columns,
    parse_timeout,
    qualified_table,
    quote_ident,
)
from contextty.safety import UnsafeSQLError


class FakeCursor:
    def __init__(self) -> None:
        self.description = [("value",)]
        self.sql = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, sql, params=()) -> None:
        self.sql = sql

    def fetchall(self):
        return [(1,)]


class FakeConn:
    def cursor(self) -> FakeCursor:
        return FakeCursor()


def test_sql_helpers_quote_and_parse() -> None:
    assert quote_ident('weird"name') == '"weird""name"'
    assert qualified_table("public", "users") == '"public"."users"'
    assert parse_timeout("500ms") == 0.5
    assert parse_timeout("2m") == 120
    assert parse_index_columns('CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email, lower(name))') == [
        "email",
        "lower(name)",
    ]


def test_introspection_sql_is_postgres_metadata_only() -> None:
    introspector = PostgresIntrospector()
    assert "pg_class" in introspector.tables_sql
    assert "information_schema.columns" in introspector.columns_sql
    assert "pg_indexes" in introspector.indexes_sql


def test_readonly_query_helper_rejects_mutation_before_execute() -> None:
    with pytest.raises(UnsafeSQLError):
        PostgresIntrospector().execute_readonly_query(FakeConn(), "drop table users")
