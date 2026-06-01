from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import quote

POSTGRES_URL_RE = re.compile(
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*[:=]\s*['\"]?(?P<url>postgres(?:ql)?://[^'\"\s]+)",
    re.IGNORECASE,
)
ENV_NAME_RE = re.compile(
    r"\b(?P<name>(?:[A-Z0-9_]*)(?:DATABASE|POSTGRES|PG)(?:[A-Z0-9_]*)(?:URL|URI|DSN)?)\b"
)
DEFAULT_NAMES = {"DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN"}
SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}
SKIPPED_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".contextty"}


def detect_project(path: str | os.PathLike[str]) -> dict[str, Any]:
    root = Path(path)
    candidates: dict[str, dict[str, Any]] = {}

    for name in DEFAULT_NAMES:
        if os.environ.get(name):
            candidates[f"postgres:{name}"] = {
                "name": _source_name(name),
                "dsn_env": name,
                "source": "environment",
                "confidence": "high",
                "connector_type": "postgres",
            }

    for file_path in _candidate_files(root):
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for match in POSTGRES_URL_RE.finditer(text):
            name = match.group("name").upper()
            candidates.setdefault(
                f"postgres:{name}",
                {
                    "name": _source_name(name),
                    "dsn_env": name,
                    "source": _relative_to_root(file_path, root),
                    "confidence": "high",
                    "connector_type": "postgres",
                },
            )

        for match in ENV_NAME_RE.finditer(text):
            name = match.group("name").upper()
            candidates.setdefault(
                f"postgres:{name}",
                {
                    "name": _source_name(name),
                    "dsn_env": name,
                    "source": _relative_to_root(file_path, root),
                    "confidence": "medium",
                    "connector_type": "postgres",
                },
            )

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

    return {
        "root": str(root.resolve()),
        "sources": sorted(
            candidates.values(),
            key=lambda item: (
                item["confidence"] != "high",
                item["connector_type"],
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
