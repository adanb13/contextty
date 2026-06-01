from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

POSTGRES_URL_RE = re.compile(
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*[:=]\s*['\"]?(?P<url>postgres(?:ql)?://[^'\"\s]+)",
    re.IGNORECASE,
)
ENV_NAME_RE = re.compile(
    r"\b(?P<name>(?:[A-Z0-9_]*)(?:DATABASE|POSTGRES|PG)(?:[A-Z0-9_]*)(?:URL|URI|DSN)?)\b"
)
DEFAULT_NAMES = {"DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN", "PG_DSN"}


def detect_project(path: str | os.PathLike[str]) -> dict[str, Any]:
    root = Path(path)
    candidates: dict[str, dict[str, Any]] = {}

    for name in DEFAULT_NAMES:
        if os.environ.get(name):
            candidates[name] = {
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
                name,
                {
                    "dsn_env": name,
                    "source": str(file_path.relative_to(root)),
                    "confidence": "high",
                    "connector_type": "postgres",
                },
            )

        for match in ENV_NAME_RE.finditer(text):
            name = match.group("name").upper()
            candidates.setdefault(
                name,
                {
                    "dsn_env": name,
                    "source": str(file_path.relative_to(root)),
                    "confidence": "medium",
                    "connector_type": "postgres",
                },
            )

    return {
        "root": str(root.resolve()),
        "sources": sorted(candidates.values(), key=lambda item: (item["confidence"] != "high", item["dsn_env"])),
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
        dirnames[:] = [
            dirname
            for dirname in dirnames
            if dirname not in {".git", ".venv", "venv", "node_modules", "__pycache__", ".contextty"}
        ]
        current = Path(current_root)
        for filename in filenames:
            path = current / filename
            if filename in names or filename.endswith((".env", ".toml", ".yaml", ".yml")):
                files.append(path)
        if len(files) > 200:
            break
    return files

