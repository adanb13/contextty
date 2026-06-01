from __future__ import annotations

import pytest

from contextty.safety import UnsafeSQLError, validate_readonly_sql


def test_validate_readonly_sql_accepts_select_and_literals() -> None:
    assert validate_readonly_sql("select 'drop table users' as text") == "select 'drop table users' as text"
    assert validate_readonly_sql("WITH rows AS (SELECT 1) SELECT * FROM rows")


@pytest.mark.parametrize(
    "sql",
    [
        "delete from users",
        "select 1; drop table users",
        "insert into users(id) values (1)",
        "set role app",
        "select nextval('users_id_seq')",
        "select * into temp copied from users",
    ],
)
def test_validate_readonly_sql_rejects_mutating_sql(sql: str) -> None:
    with pytest.raises(UnsafeSQLError):
        validate_readonly_sql(sql)
