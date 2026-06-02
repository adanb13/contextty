from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .facts import facts_from_pills, hashed_vector, prepare_fact_for_storage, tokenize, vector_similarity
from .models import CONNECTOR_TYPES, DSN_ENV_CONNECTOR_TYPES, PATH_CONNECTOR_TYPES, SnapshotRun, Source

DEFAULT_STORE_PATH = Path(".contextty") / "contextty.db"
SCHEMA_VERSION = 4
CONNECTOR_TYPE_SQL = ", ".join(f"'{connector_type}'" for connector_type in CONNECTOR_TYPES)


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
                    connector_type TEXT NOT NULL CHECK (connector_type IN ('postgres', 'sqlite', 'mysql', 'mariadb', 'duckdb')),
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

                CREATE TABLE IF NOT EXISTS facts (
                    id TEXT PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    snapshot_run_id INTEGER NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
                    node_id TEXT,
                    kind TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    text TEXT NOT NULL,
                    data_json TEXT NOT NULL DEFAULT '{}',
                    search_text TEXT NOT NULL,
                    vector_json TEXT NOT NULL DEFAULT '[]'
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
                CREATE INDEX IF NOT EXISTS idx_facts_latest
                    ON facts(source_id, snapshot_run_id, kind);
                """
            )
            conn.commit()
            self._ensure_sources_connector_type_check(conn)
            self._ensure_facts_fts(conn)
            conn.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )

    @staticmethod
    def _ensure_sources_connector_type_check(conn: sqlite3.Connection) -> None:
        row = conn.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'sources'").fetchone()
        table_sql = row["sql"] if row else ""
        if all(f"'{connector_type}'" in table_sql for connector_type in CONNECTOR_TYPES):
            return

        conn.execute("PRAGMA foreign_keys = OFF")
        conn.executescript(
            f"""
            CREATE TABLE sources_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                connector_type TEXT NOT NULL CHECK (connector_type IN ({CONNECTOR_TYPE_SQL})),
                dsn_env TEXT,
                path TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                metadata_json TEXT NOT NULL DEFAULT '{{}}'
            );

            INSERT INTO sources_new(id, name, connector_type, dsn_env, path, created_at, updated_at, metadata_json)
            SELECT id, name, connector_type, dsn_env, path, created_at, updated_at, metadata_json
            FROM sources;

            DROP TABLE sources;
            ALTER TABLE sources_new RENAME TO sources;
            """
        )
        conn.execute("PRAGMA foreign_keys = ON")

    @staticmethod
    def _ensure_facts_fts(conn: sqlite3.Connection) -> bool:
        try:
            conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(fact_id UNINDEXED, search_text)")
            return True
        except sqlite3.OperationalError:
            return False

    def add_source(
        self,
        name: str,
        connector_type: str,
        dsn_env: str | None = None,
        path: str | os.PathLike[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Source:
        if connector_type in DSN_ENV_CONNECTOR_TYPES:
            if not dsn_env:
                raise ValueError(f"{connector_type} sources require dsn_env")
            path_value = None
            dsn_env_value: str | None = dsn_env
        elif connector_type in PATH_CONNECTOR_TYPES:
            if not path:
                raise ValueError(f"{connector_type} sources require path")
            path_value = str(Path(path).expanduser().resolve())
            dsn_env_value = None
        else:
            raise ValueError(f"connector_type must be one of: {', '.join(CONNECTOR_TYPES)}")
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
        facts: Iterable[dict[str, Any]] | None = None,
    ) -> None:
        node_rows = list(nodes)
        edge_rows = list(edges)
        pill_rows = list(pills)
        fact_rows = [prepare_fact_for_storage(fact) for fact in facts] if facts is not None else facts_from_pills(pill_rows)
        with self.connect() as conn:
            self._delete_facts_fts(conn, source_id)
            conn.execute(
                "DELETE FROM facts WHERE source_id = ?",
                (source_id,),
            )
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
                node_rows,
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
                edge_rows,
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
                pill_rows,
            )
            conn.executemany(
                """
                INSERT INTO facts(
                    id, source_id, snapshot_run_id, node_id, kind, subject, text,
                    data_json, search_text, vector_json
                )
                VALUES (
                    :id, :source_id, :snapshot_run_id, :node_id, :kind, :subject, :text,
                    :data_json, :search_text, :vector_json
                )
                """,
                fact_rows,
            )
            self._insert_facts_fts(conn, fact_rows)

    @staticmethod
    def _delete_facts_fts(conn: sqlite3.Connection, source_id: int) -> None:
        try:
            conn.execute(
                """
                DELETE FROM facts_fts
                WHERE fact_id IN (SELECT id FROM facts WHERE source_id = ?)
                """,
                (source_id,),
            )
        except sqlite3.OperationalError:
            return

    @staticmethod
    def _insert_facts_fts(conn: sqlite3.Connection, facts: list[dict[str, Any]]) -> None:
        if not facts:
            return
        try:
            conn.executemany(
                "INSERT INTO facts_fts(fact_id, search_text) VALUES (?, ?)",
                [(fact["id"], fact["search_text"]) for fact in facts],
            )
        except sqlite3.OperationalError:
            return

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

    def get_facts(
        self,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
        kind: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        run = self._resolve_run(source_id, snapshot_run_id)
        if run is None:
            return []
        query = "SELECT * FROM facts WHERE snapshot_run_id = ?"
        params: list[Any] = [run.id]
        if kind is not None:
            query += " AND kind = ?"
            params.append(kind)
        query += " ORDER BY kind, subject"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_fact(row) for row in rows]

    def search_facts(
        self,
        query: str,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
        limit: int = 16,
    ) -> list[dict[str, Any]]:
        run = self._resolve_run(source_id, snapshot_run_id)
        if run is None:
            return []
        facts = self.get_facts(snapshot_run_id=run.id)
        if not facts:
            return []

        fts_scores = self._search_facts_fts(query, run.id, limit=max(limit * 8, 64))
        query_tokens = set(tokenize(query))
        query_vector = hashed_vector(query)
        scored: list[tuple[dict[str, Any], float]] = []
        normalized_query = " ".join(query.lower().split())
        for fact in facts:
            search_text = fact.get("search_text") or fact.get("text") or ""
            fact_tokens = set(tokenize(search_text))
            overlap = query_tokens.intersection(fact_tokens)
            vector = fact.get("vector") if isinstance(fact.get("vector"), list) else []
            score = len(overlap) * 8.0
            score += vector_similarity(query_vector, vector) * 6.0
            if fact["id"] in fts_scores:
                score += 5.0 + fts_scores[fact["id"]]
            if normalized_query and normalized_query in search_text.lower():
                score += 8.0
            for phrase in quoted_phrases(query):
                if phrase.lower() in search_text.lower():
                    score += 12.0
            if score > 0:
                ranked = dict(fact)
                ranked["score"] = round(score, 6)
                scored.append((ranked, score))
        scored.sort(key=lambda item: (-item[1], item[0]["kind"], item[0]["subject"]))
        return [fact for fact, _score in scored[:limit]]

    def _search_facts_fts(self, query: str, snapshot_run_id: int, limit: int) -> dict[str, float]:
        fts_query = fts_query_for(query)
        if not fts_query:
            return {}
        try:
            with self.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT f.id, bm25(facts_fts) AS rank
                    FROM facts_fts
                    JOIN facts f ON f.id = facts_fts.fact_id
                    WHERE facts_fts MATCH ?
                      AND f.snapshot_run_id = ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (fts_query, snapshot_run_id, limit),
                ).fetchall()
        except sqlite3.OperationalError:
            return {}
        scores: dict[str, float] = {}
        for row in rows:
            rank = row["rank"]
            scores[row["id"]] = 1.0 / (1.0 + abs(float(rank or 0.0)))
        return scores

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        return self._row_to_node(row) if row is not None else None

    def resolve_snapshot_run(
        self,
        source_id: int | None = None,
        snapshot_run_id: int | None = None,
    ) -> SnapshotRun | None:
        return self._resolve_run(source_id, snapshot_run_id)

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

    @staticmethod
    def _row_to_fact(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "source_id": row["source_id"],
            "snapshot_run_id": row["snapshot_run_id"],
            "node_id": row["node_id"],
            "kind": row["kind"],
            "subject": row["subject"],
            "text": row["text"],
            "data": loads_json(row["data_json"]),
            "search_text": row["search_text"],
            "vector": loads_json(row["vector_json"]),
        }


def quoted_phrases(query: str) -> list[str]:
    return [match.group(1) or match.group(2) for match in re.finditer(r"'([^']+)'|\"([^\"]+)\"", query)]


def fts_query_for(query: str) -> str:
    tokens = []
    for token in tokenize(query):
        if re.fullmatch(r"[A-Za-z0-9_]+", token):
            tokens.append(token)
        if len(tokens) >= 12:
            break
    return " OR ".join(f"{token}*" for token in tokens)
