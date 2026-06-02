from __future__ import annotations

from .mysql import MySQLConnector, MySQLIntrospector, parse_mysql_dsn

MARIADB_SCHEMES = {"mariadb", "mariadb+pymysql"}


class MariaDBConnector(MySQLConnector):
    schemes = MARIADB_SCHEMES


class MariaDBIntrospector(MySQLIntrospector):
    pass


def parse_mariadb_dsn(dsn: str) -> dict[str, object]:
    return parse_mysql_dsn(dsn, allowed_schemes=MARIADB_SCHEMES)
