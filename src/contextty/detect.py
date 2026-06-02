from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from .models import CONNECTOR_TYPES

DATABASE_URL_RE = re.compile(
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*[:=]\s*['\"]?(?P<url>(?:postgres(?:ql)?|mysql(?:\+pymysql)?|mariadb(?:\+pymysql)?)://[^'\"\s]+)",
    re.IGNORECASE,
)
ENV_NAME_RE = re.compile(
    r"\b(?P<name>[A-Z0-9_]*(?:DATABASE|POSTGRES|PG|MYSQL|MARIADB)[A-Z0-9_]*(?:URL|URI|DSN)?)\b"
)
POSTGRES_NAMES = {"DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN"}
MYSQL_NAMES = {"MYSQL_URL", "MYSQL_DATABASE_URL", "MYSQL_DSN"}
MARIADB_NAMES = {"MARIADB_URL", "MARIADB_DATABASE_URL", "MARIADB_DSN"}
URL_SCHEME_CONNECTORS = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mysql+pymysql": "mysql",
    "mariadb": "mariadb",
    "mariadb+pymysql": "mariadb",
}
CONNECTOR_SORT_ORDER = {connector_type: index for index, connector_type in enumerate(CONNECTOR_TYPES)}
SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}
DUCKDB_SUFFIXES = {".duckdb", ".ddb"}
SKIPPED_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".contextty"}


def detect_project(path: str | os.PathLike[str]) -> dict[str, Any]:
    root = Path(path)
    candidates: dict[str, dict[str, Any]] = {}

    for name in sorted(POSTGRES_NAMES | MYSQL_NAMES | MARIADB_NAMES):
        value = os.environ.get(name)
        if not value:
            continue
        connector_type = _connector_from_url(value) or _connector_from_env_name(name)
        if connector_type:
            _add_dsn_candidate(candidates, connector_type, name, "environment", "high")

    for file_path in _candidate_files(root):
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        url_names: set[str] = set()
        for match in DATABASE_URL_RE.finditer(text):
            name = match.group("name").upper()
            connector_type = _connector_from_url(match.group("url"))
            if not connector_type:
                continue
            url_names.add(name)
            _add_dsn_candidate(candidates, connector_type, name, _relative_to_root(file_path, root), "high")

        for match in ENV_NAME_RE.finditer(text):
            name = match.group("name").upper()
            if name in url_names:
                continue
            connector_type = _connector_from_env_name(name)
            if connector_type:
                _add_dsn_candidate(candidates, connector_type, name, _relative_to_root(file_path, root), "medium")

    for file_path in _candidate_sqlite_files(root):
        if not _is_readable_sqlite(file_path):
            continue
        resolved = str(file_path.resolve())
        candidates.setdefault(
            f"sqlite:{resolved}",
            {
                "name": _source_name(file_path.stem),
                "path": resolved,
                "source": _relative_to_root(file_path, root),
                "confidence": "high",
                "connector_type": "sqlite",
            },
        )

    for file_path in _candidate_duckdb_files(root):
        if not _is_readable_duckdb(file_path):
            continue
        resolved = str(file_path.resolve())
        candidates.setdefault(
            f"duckdb:{resolved}",
            {
                "name": _source_name(file_path.stem),
                "path": resolved,
                "source": _relative_to_root(file_path, root),
                "confidence": "high",
                "connector_type": "duckdb",
            },
        )

    return {
        "root": str(root.resolve()),
        "sources": sorted(
            candidates.values(),
            key=lambda item: (
                item["confidence"] != "high",
                CONNECTOR_SORT_ORDER.get(item["connector_type"], 999),
                item.get("dsn_env") or item.get("path") or "",
            ),
        ),
    }


def _candidate_files(root: Path) -> list[Path]:
    names = {
        ".env",
        ".env.local",
        ".env.development",
        ".env.test",
        "docker-compose.yml",
        "docker-compose.yaml",
        "compose.yml",
        "compose.yaml",
        "settings.py",
        "config.py",
        "pyproject.toml",
    }
    files: list[Path] = []
    if root.is_file():
        return [root]
    for current_root, dirnames, filenames in os.walk(root):
        dirnames[:] = [dirname for dirname in dirnames if dirname not in SKIPPED_DIRS]
        current = Path(current_root)
        for filename in filenames:
            path = current / filename
            if filename in names or filename.endswith((".env", ".toml", ".yaml", ".yml")):
                files.append(path)
        if len(files) > 200:
            break
    return files


def _candidate_sqlite_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() in SQLITE_SUFFIXES else []

    files: list[Path] = []
    for current_root, dirnames, filenames in os.walk(root):
        dirnames[:] = [dirname for dirname in dirnames if dirname not in SKIPPED_DIRS]
        current = Path(current_root)
        for filename in filenames:
            path = current / filename
            if path.suffix.lower() in SQLITE_SUFFIXES:
                files.append(path)
        if len(files) > 1000:
            break
    return files


def _candidate_duckdb_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() in DUCKDB_SUFFIXES else []

    files: list[Path] = []
    for current_root, dirnames, filenames in os.walk(root):
        dirnames[:] = [dirname for dirname in dirnames if dirname not in SKIPPED_DIRS]
        current = Path(current_root)
        for filename in filenames:
            path = current / filename
            if path.suffix.lower() in DUCKDB_SUFFIXES:
                files.append(path)
        if len(files) > 1000:
            break
    return files


def _is_readable_sqlite(path: Path) -> bool:
    try:
        uri = f"file:{quote(str(path.resolve()), safe='/:')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=0.2)
        try:
            conn.execute("PRAGMA query_only = ON")
            conn.execute("PRAGMA schema_version").fetchone()
            return True
        finally:
            conn.close()
    except sqlite3.Error:
        return False
    except OSError:
        return False


def _is_readable_duckdb(path: Path) -> bool:
    try:
        import duckdb
    except ModuleNotFoundError:
        return False

    try:
        conn = duckdb.connect(str(path.resolve()), read_only=True)
        try:
            conn.execute("SELECT 1").fetchone()
            return True
        finally:
            conn.close()
    except Exception:
        return False


def _add_dsn_candidate(
    candidates: dict[str, dict[str, Any]],
    connector_type: str,
    name: str,
    source: str,
    confidence: str,
) -> None:
    candidates.setdefault(
        f"{connector_type}:{name}",
        {
            "name": _source_name(name),
            "dsn_env": name,
            "source": source,
            "confidence": confidence,
            "connector_type": connector_type,
        },
    )


def _connector_from_url(value: str) -> str | None:
    return URL_SCHEME_CONNECTORS.get(urlparse(value).scheme.lower())


def _connector_from_env_name(name: str) -> str | None:
    if name in MYSQL_NAMES or "MYSQL" in name:
        return "mysql"
    if name in MARIADB_NAMES or "MARIADB" in name:
        return "mariadb"
    if name in POSTGRES_NAMES or "POSTGRES" in name or name.startswith("PG_") or "_PG_" in name:
        return "postgres"
    return None


def _relative_to_root(path: Path, root: Path) -> str:
    if root.is_file():
        return path.name
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _source_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9]+", "-", value.lower()).strip("-")
    return name or "source"
