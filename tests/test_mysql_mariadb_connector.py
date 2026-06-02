from __future__ import annotations

import pytest

from contextty.connectors.mariadb import MariaDBConnector, MariaDBIntrospector, parse_mariadb_dsn
from contextty.connectors.mysql import (
    MySQLIntrospector,
    UnsupportedDSNError,
    group_index_rows,
    mysql_type_kind,
    parse_mysql_dsn,
    qualified_table,
    quote_ident,
)
from contextty.safety import UnsafeSQLError


class FakeCursor:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, sql, params=()) -> None:
        if "information_schema.tables" in sql:
            self.rows = [
                {
                    "database_name": "app",
                    "schema_name": "app",
                    "table_name": "users",
                    "kind": "table",
                    "row_estimate": 12,
                    "size_bytes": 4096,
                    "comment": "application users",
                },
                {
                    "database_name": "app",
                    "schema_name": "app",
                    "table_name": "verified_users",
                    "kind": "view",
                    "row_estimate": None,
                    "size_bytes": None,
                    "comment": None,
                },
            ]
        elif "information_schema.columns" in sql:
            self.rows = [
                {
                    "database_name": "app",
                    "table_schema": "app",
                    "table_name": "users",
                    "column_name": "id",
                    "ordinal_position": 1,
                    "data_type": "int",
                    "is_nullable": "NO",
                    "column_default": None,
                    "character_maximum_length": None,
                    "numeric_precision": 10,
                    "numeric_scale": 0,
                    "comment": None,
                },
                {
                    "database_name": "app",
                    "table_schema": "app",
                    "table_name": "users",
                    "column_name": "email",
                    "ordinal_position": 2,
                    "data_type": "varchar",
                    "is_nullable": "NO",
                    "column_default": None,
                    "character_maximum_length": 255,
                    "numeric_precision": None,
                    "numeric_scale": None,
                    "comment": "login email",
                },
            ]
        elif "PRIMARY KEY" in sql:
            self.rows = [
                {
                    "database_name": "app",
                    "table_schema": "app",
                    "table_name": "users",
                    "column_name": "id",
                    "ordinal_position": 1,
                    "constraint_name": "PRIMARY",
                }
            ]
        elif "FOREIGN KEY" in sql:
            self.rows = []
        elif "information_schema.statistics" in sql:
            self.rows = [
                {
                    "database_name": "app",
                    "schema_name": "app",
                    "table_name": "users",
                    "index_name": "PRIMARY",
                    "non_unique": 0,
                    "seq_in_index": 1,
                    "column_name": "id",
                    "index_type": "BTREE",
                },
                {
                    "database_name": "app",
                    "schema_name": "app",
                    "table_name": "users",
                    "index_name": "users_email_idx",
                    "non_unique": 0,
                    "seq_in_index": 1,
                    "column_name": "email",
                    "index_type": "BTREE",
                },
            ]
        elif "information_schema.views" in sql:
            self.rows = [
                {
                    "database_name": "app",
                    "table_schema": "app",
                    "table_name": "verified_users",
                    "view_definition": "select id, email from users",
                }
            ]
        else:
            self.rows = [{"value": 1}]

    def fetchall(self):
        return self.rows


class FakeConn:
    def cursor(self) -> FakeCursor:
        return FakeCursor()


def test_mysql_dsn_parsing_quoting_and_type_classification() -> None:
    args = parse_mysql_dsn("mysql+pymysql://alice:p%40ss@db.example.com:3307/app_db?charset=utf8mb4")

    assert args["host"] == "db.example.com"
    assert args["port"] == 3307
    assert args["user"] == "alice"
    assert args["password"] == "p@ss"
    assert args["database"] == "app_db"
    assert quote_ident("weird`name") == "`weird``name`"
    assert qualified_table("app", "users") == "`app`.`users`"
    assert mysql_type_kind("decimal") == "numeric"
    assert mysql_type_kind("timestamp") == "time"
    assert mysql_type_kind("varchar") == "text"

    with pytest.raises(UnsupportedDSNError):
        parse_mysql_dsn("postgresql://user:pass@localhost/app")


def test_mariadb_dsn_parsing_uses_mariadb_schemes() -> None:
    args = parse_mariadb_dsn("mariadb+pymysql://root:secret@localhost/warehouse")

    assert args["host"] == "localhost"
    assert args["database"] == "warehouse"
    assert MariaDBConnector.schemes == {"mariadb", "mariadb+pymysql"}
    assert issubclass(MariaDBIntrospector, MySQLIntrospector)

    with pytest.raises(UnsupportedDSNError):
        parse_mariadb_dsn("mysql://root:secret@localhost/warehouse")


def test_mysql_metadata_sql_and_index_grouping() -> None:
    introspector = MySQLIntrospector()

    assert "information_schema.tables" in introspector.tables_sql
    assert "information_schema.statistics" in introspector.indexes_sql
    assert "DATABASE()" in introspector.columns_sql

    indexes = group_index_rows(
        [
            {
                "database_name": "app",
                "schema_name": "app",
                "table_name": "orders",
                "index_name": "orders_user_status_idx",
                "non_unique": 1,
                "seq_in_index": 2,
                "column_name": "status",
            },
            {
                "database_name": "app",
                "schema_name": "app",
                "table_name": "orders",
                "index_name": "orders_user_status_idx",
                "non_unique": 1,
                "seq_in_index": 1,
                "column_name": "user_id",
            },
        ]
    )

    assert indexes[0].columns == ["user_id", "status"]
    assert indexes[0].unique is False


def test_mysql_introspection_maps_metadata_rows() -> None:
    inspection = MySQLIntrospector().inspect(FakeConn())

    assert inspection.database == "app"
    assert {(table.schema, table.name, table.kind) for table in inspection.tables} >= {
        ("app", "users", "table"),
        ("app", "verified_users", "view"),
    }
    assert {(column.table, column.name, column.data_type, column.nullable) for column in inspection.columns} >= {
        ("users", "id", "int", False),
        ("users", "email", "varchar", False),
    }
    assert [(pk.table, pk.column, pk.ordinal) for pk in inspection.primary_keys] == [("users", "id", 1)]
    assert any(index.name == "users_email_idx" and index.columns == ["email"] for index in inspection.indexes)
    assert any(view.name == "verified_users" and "select id" in (view.definition or "") for view in inspection.views)


def test_mysql_readonly_query_helper_rejects_mutation_before_execute() -> None:
    with pytest.raises(UnsafeSQLError):
        MySQLIntrospector().execute_readonly_query(FakeConn(), "drop table users")
