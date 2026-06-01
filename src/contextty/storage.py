from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .models import SnapshotRun, Source

DEFAULT_STORE_PATH = Path(".contextty") / "contextty.db"
SCHEMA_VERSION = 2


def default_store_path() -> Path:
    override = os.environ.get("CONTEXTTY_STORE_PATH")
    if override:
        return Path(override)
    return DEFAULT_STORE_PATH


def dumps_json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, sort_keys=True, default=str)


def loads_json(value: str | None) -> Any:
    if not value:
        return {}
    return json.loads(value)


class LocalStore:
    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path is not None else default_store_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    connector_type TEXT NOT NULL CHECK (connector_type IN ('postgres', 'sqlite')),
                    dsn_env TEXT,
                    path TEXT,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS snapshot_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    finished_at TEXT,
                    profile_mode TEXT NOT NULL,
                    row_limit INTEGER NOT NULL,
                    timeout_seconds REAL NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    snapshot_run_id INTEGER NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    qualified_name TEXT NOT NULL,
                    parent_id TEXT,
                    summary TEXT,
                    properties_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS edges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    snapshot_run_id INTEGER NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
                    from_node_id TEXT NOT NULL,
                    to_node_id TEXT NOT NULL,
                    relation TEXT NOT NULL,
                    properties_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS pills (
                    id TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    snapshot_run_id INTEGER NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
                    node_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    json TEXT NOT NULL DEFAULT '{}',
                    rendered_text TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_snapshot_runs_source
                    ON snapshot_runs(source_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_nodes_latest
                    ON nodes(source_id, snapshot_run_id, kind);
                CREATE INDEX IF NOT EXISTS idx_nodes_qualified_name
                    ON nodes(qualified_name);
                CREATE INDEX IF NOT EXISTS idx_edges_from
                    ON edges(source_id, snapshot_run_id, from_node_id);
                CREATE INDEX IF NOT EXISTS idx_edges_to
                    ON edges(source_id, snapshot_run_id, to_node_id);
                CREATE INDEX IF NOT EXISTS idx_pills_node
                    ON pills(source_id, snapshot_run_id, node_id);
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )

    def add_source(
        self,
        name: str,
        connector_type: str,
        dsn_env: str | None = None,
        path: str | os.PathLike[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Source:
        if connector_type == "postgres":
            if not dsn_env:
                raise ValueError("Postgres sources require dsn_env")
            path_value = None
            dsn_env_value: str | None = dsn_env
        elif connector_type == "sqlite":
            if not path:
                raise ValueError("SQLite sources require path")
            path_value = str(Path(path).expanduser().resolve())
            dsn_env_value = None
        else:
            raise ValueError("connector_type must be one of: postgres, sqlite")
        metadata = metadata or {}
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO sources(name, connector_type, dsn_env, path, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    connector_type = excluded.connector_type,
                    dsn_env = excluded.dsn_env,
                    path = excluded.path,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    metadata_json = excluded.metadata_json
                """,
                (name, connector_type, dsn_env_value, path_value, dumps_json(metadata)),
            )
        return self.get_source(name)

    def list_sources(self) -> list[Source]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM sources ORDER BY name").fetchall()
        return [self._row_to_source(row) for row in rows]

    def get_source(self, name: str) -> Source:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise KeyError(f"source not found: {name}")
        return self._row_to_source(row)

    def create_snapshot_run(
        self,
        source_id: int,
        profile_mode: str,
        row_limit: int,
        timeout_seconds: float,
        status: str = "running",
        metadata: dict[str, Any] | None = None,
    ) -> SnapshotRun:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO snapshot_runs(
                    source_id, profile_mode, row_limit, timeout_seconds, status, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    profile_mode,
                    row_limit,
                    timeout_seconds,
                    status,
                    dumps_json(metadata or {}),
                ),
            )
            row_id = cur.lastrowid
            row = conn.execute("SELECT * FROM snapshot_runs WHERE id = ?", (row_id,)).fetchone()
        return self._row_to_snapshot(row)

    def finish_snapshot_run(
        self,
        run_id: int,
        status: str,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SnapshotRun:
        metadata_json = dumps_json(metadata or {})
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE snapshot_runs
                SET finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    status = ?,
                    error = ?,
                    metadata_json = ?
                WHERE id = ?
                """,
                (status, error, metadata_json, run_id),
            )
            row = conn.execute("SELECT * FROM snapshot_runs WHERE id = ?", (run_id,)).fetchone()
        return self._row_to_snapshot(row)

    def latest_snapshot_run(self, source_id: int | None = None) -> SnapshotRun | None:
        query = "SELECT * FROM snapshot_runs WHERE status = 'success'"
        params: list[Any] = []
        if source_id is not None:
            query += " AND source_id = ?"
            params.append(source_id)
        query += " ORDER BY id DESC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_snapshot(row) if row is not None else None

    def replace_artifact(
        self,
        source_id: int,
        snapshot_run_id: int,
        nodes: Iterable[dict[str, Any]],
        edges: Iterable[dict[str, Any]],
        pills: Iterable[dict[str, Any]],
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM pills WHERE source_id = ?",
                (source_id,),
            )
            conn.execute(
                "DELETE FROM edges WHERE source_id = ?",
                (source_id,),
            )
            conn.execute(
                "DELETE FROM nodes WHERE source_id = ?",
                (source_id,),
            )
            conn.executemany(
                """
                INSERT INTO nodes(
                    id, source_id, snapshot_run_id, kind, name, qualified_name,
                    parent_id, summary, properties_json
                )
                VALUES (
                    :id, :source_id, :snapshot_run_id, :kind, :name, :qualified_name,
                    :parent_id, :summary, :properties_json
                )
                """,
                list(nodes),
            )
            conn.executemany(
                """
                INSERT INTO edges(
                    source_id, snapshot_run_id, from_node_id, to_node_id, relation, properties_json
                )
                VALUES (
                    :source_id, :snapshot_run_id, :from_node_id, :to_node_id, :relation, :properties_json
                )
                """,
                list(edges),
            )
            conn.executemany(
                """
                INSERT INTO pills(
                    id, source_id, snapshot_run_id, node_id, kind, title, json, rendered_text
                )
                VALUES (
                    :id, :source_id, :snapshot_run_id, :node_id, :kind, :title, :json, :rendered_text
                )
                """,
                list(pills),
            )

    def get_nodes(
        self,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        run = self._resolve_run(source_id, snapshot_run_id)
        if run is None:
            return []
        query = "SELECT * FROM nodes WHERE snapshot_run_id = ?"
        params: list[Any] = [run.id]
        query += " ORDER BY kind, qualified_name"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_node(row) for row in rows]

    def get_edges(
        self,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
    ) -> list[dict[str, Any]]:
        run = self._resolve_run(source_id, snapshot_run_id)
        if run is None:
            return []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM edges WHERE snapshot_run_id = ? ORDER BY from_node_id, to_node_id, relation",
                (run.id,),
            ).fetchall()
        return [self._row_to_edge(row) for row in rows]

    def get_pills(
        self,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
        node_id: str | None = None,
    ) -> list[dict[str, Any]]:
        run = self._resolve_run(source_id, snapshot_run_id)
        if run is None:
            return []
        query = "SELECT * FROM pills WHERE snapshot_run_id = ?"
        params: list[Any] = [run.id]
        if node_id is not None:
            query += " AND node_id = ?"
            params.append(node_id)
        query += " ORDER BY kind, title"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_pill(row) for row in rows]

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        return self._row_to_node(row) if row is not None else None

    def source_for_run(self, run_id: int) -> Source:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT s.*
                FROM sources s
                JOIN snapshot_runs r ON r.source_id = s.id
                WHERE r.id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"snapshot run not found: {run_id}")
        return self._row_to_source(row)

    def _resolve_run(
        self,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
    ) -> SnapshotRun | None:
        if snapshot_run_id is not None:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM snapshot_runs WHERE id = ?",
                    (snapshot_run_id,),
                ).fetchone()
            return self._row_to_snapshot(row) if row is not None else None
        return self.latest_snapshot_run(source_id)

    @staticmethod
    def _row_to_source(row: sqlite3.Row) -> Source:
        return Source(
            id=row["id"],
            name=row["name"],
            connector_type=row["connector_type"],
            dsn_env=row["dsn_env"],
            path=row["path"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            metadata=loads_json(row["metadata_json"]),
        )

    @staticmethod
    def _row_to_snapshot(row: sqlite3.Row) -> SnapshotRun:
        return SnapshotRun(
            id=row["id"],
            source_id=row["source_id"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            profile_mode=row["profile_mode"],
            row_limit=row["row_limit"],
            timeout_seconds=row["timeout_seconds"],
            status=row["status"],
            error=row["error"],
            metadata=loads_json(row["metadata_json"]),
        )

    @staticmethod
    def _row_to_node(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "source_id": row["source_id"],
            "snapshot_run_id": row["snapshot_run_id"],
            "kind": row["kind"],
            "name": row["name"],
            "qualified_name": row["qualified_name"],
            "parent_id": row["parent_id"],
            "summary": row["summary"],
            "properties": loads_json(row["properties_json"]),
        }

    @staticmethod
    def _row_to_edge(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "source_id": row["source_id"],
            "snapshot_run_id": row["snapshot_run_id"],
            "from_node_id": row["from_node_id"],
            "to_node_id": row["to_node_id"],
            "relation": row["relation"],
            "properties": loads_json(row["properties_json"]),
        }

    @staticmethod
    def _row_to_pill(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "source_id": row["source_id"],
            "snapshot_run_id": row["snapshot_run_id"],
            "node_id": row["node_id"],
            "kind": row["kind"],
            "title": row["title"],
            "json": loads_json(row["json"]),
            "rendered_text": row["rendered_text"],
        }
