# Contextty

Give AI agents a local, database-agnostic map of your data sources without
giving them the database.

Contextty turns approved database sources into local, AI-readable context
artifacts through isolated connectors. It captures schema metadata,
relationship graphs, bounded profiling summaries, and compact answer facts so
agents can answer common data questions through the CLI, HTTP API, or MCP
without querying live sources during normal context lookup.

## Why Contextty

- Database-agnostic connector architecture: isolate each source type's
  connection, introspection, SQL dialect, and profiling behavior behind a
  dedicated connector.
- Local snapshots: build a `.contextty/contextty.db` artifact that agents can
  query without live source access.
- Read-only, bounded access: inspect and snapshot operations use read-only
  connections, SQL guardrails, statement timeouts, and row limits.
- CLI, API, and MCP access: expose the same registered sources and local
  context artifacts through terminal commands, HTTP endpoints, and MCP tools.

## Benefits For AI Agents

Contextty gives agents local database awareness without handing them
production credentials or requiring repeated live SQL queries for normal
context lookup. The artifact is a semantic/context snapshot, not a compressed
database copy, backup, or table dump.

- Lightweight context: captures schema, relationships, indexes, views, and
  bounded profile summaries in an AI-readable local artifact.
- Safer default workflow: agents query the local artifact after refresh instead
  of connecting to the live database for every context question.
- Token-efficient retrieval: a local fact index answers common questions first,
  with graph search and word-budgeted rendering as fallback context.
- Lower database load: context lookup is local after an approved snapshot
  refresh.
- Relationship-aware answers: connected tables, columns, foreign keys, and
  indexes stay available as graph context.
- Connector-neutral output: supported databases render into the same local
  context model.

## Quickstart

Install Contextty:

```bash
pip install contextty
```

Detect database sources in a project:

```bash
contextty detect .
```

Register an approved source with the connector and locator fields for that
database type. For example, a source whose connector reads a DSN from an
environment variable:

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/app'
contextty source add app-db --type postgres --dsn-env DATABASE_URL
```

Or a source whose connector reads a local database file:

```bash
contextty source add local-db --type sqlite --path ./app.sqlite3
```

Inspect a source, refresh a snapshot, and query the local artifact:

```bash
contextty inspect app-db
contextty snapshot app-db --profile-mode deep --row-limit 10000 --timeout 5s
contextty query "what tables explain signup state?" --source app-db --budget 2000
```

Serve the local HTTP API:

```bash
contextty serve --api --host 127.0.0.1 --port 8765
curl -s http://127.0.0.1:8765/v1/sources
```

Serve Contextty over MCP stdio:

```bash
contextty serve --mcp
```

## What Contextty Captures

Contextty snapshots are database context, not database backups or compressed
database copies. A snapshot can include:

- Schemas, tables, views, and view definitions.
- Columns, data types, nullability, and defaults.
- Primary keys, foreign keys, indexes, and relationship edges.
- Database, schema, table, view, column, index, and relationship graph nodes.
- Context pills that summarize useful local facts for AI agents.
- Basic profiling summaries, with optional deep profiling over a bounded row
  sample that can include sampled or top values.
- A local fact index backed by lexical lookup and deterministic vector
  reranking. Deep snapshots can add bounded row-derived facts such as entity
  labels, foreign-key relationships, bridge assignments, latest metrics, and
  low-risk grouped aggregates.

After a snapshot is built, `contextty query` searches the local fact index
first. If no answer-ready fact matches, it falls back to compact graph context
with the involved tables and columns.

## How Contextty Works

Contextty separates database-specific extraction from connector-neutral
retrieval.

1. **Connector-specific inspection** reads an approved source through an
   isolated connector. Each connector owns its connection method, SQL dialect,
   schema introspection, profiling queries, and read-only guardrails.
2. **Connector-neutral artifact graph** converts schemas, tables, views,
   columns, indexes, primary keys, and foreign keys into the same local
   node/edge artifact regardless of database type.
3. **Compact fact extraction** turns schema and bounded profile data into
   answer-ready facts: table inventories, table schemas, value domains,
   relationship cards, entity labels, foreign-key relationships, bridge
   assignments, latest metrics, and grouped aggregates.
4. **Deterministic retrieval** uses `query_context` to search the local fact
   index first with normalized tokens, lexical overlap, phrase matching, schema
   routing hints, and deterministic hashed-vector reranking.
5. **Budgeted context rendering** returns compact `answer_candidates` and
   context within the requested budget. If no bounded snapshot fact can answer
   a row-level question, `query_context` marks the result as needing database
   fallback instead of querying the live database.

In generated benchmark suites, Contextty matched direct database answers while
answering from the local snapshot without fallback.

Each new database type can have its own connector and context extractor for
source-specific inspection and profiling, while reusing the shared artifact
builder, fact model, local fact index, graph traversal, answerability status,
rendering strategy, and shared CLI, API, and MCP query behavior.

## Current Connectors

| Connector | Status | Registration | Notes |
| --- | --- | --- | --- |
| PostgreSQL | Available now | `--type postgres --dsn-env DATABASE_URL` | Connects through a DSN stored in an environment variable. |
| SQLite | Available now | `--type sqlite --path ./app.sqlite3` | Connects to a local SQLite database file. |
| Future connectors | Connector model ready | Connector-specific fields | New database types live in dedicated connector modules with isolated dialect logic. |

## Use Contextty From

- CLI: run `contextty detect`, `source add`, `inspect`, `snapshot`, `query`,
  `graph`, and `serve` from a terminal.
- HTTP API: run `contextty serve --api` for local `/v1/sources`,
  `/v1/detect`, `/v1/inspect`, `/v1/snapshot`, `/v1/query`, `/v1/graph`, and
  graph node endpoints.
- MCP: run `contextty serve --mcp` so MCP clients can call `detect_sources`,
  `add_source`, `list_sources`, `inspect_source`, `refresh_snapshot`,
  `query_context`, `get_node`, `get_neighbors`, and `find_path`.

## Safety Model

Contextty is built around approved sources and local artifacts:

- Sources must be registered before inspection or snapshot refresh.
- Initial inspection and snapshot refresh still require approved read-only
  access to the source.
- Connectors default to read-only database access.
- User-provided SQL goes through the shared read-only guard before execution.
- Connection waits and query execution are bounded with timeouts.
- Profiling is bounded by `SnapshotOptions.row_limit`.
- Row-derived facts are capped and truncated; Contextty does not store full row
  blobs or arbitrary table replicas.
- `contextty query` reads the local snapshot only; it does not execute SQL
  against the live database.
- Snapshots can become stale until they are refreshed.
- Deep profiling can store sampled or top values in the artifact, so access to
  `.contextty/contextty.db` should still be treated as sensitive.

## Command Reference

These commands cover source registration and snapshot workflows. The source
registration examples use currently available connectors:

```bash
contextty detect .
contextty source add app-db --type postgres --dsn-env DATABASE_URL
contextty source add local-db --type sqlite --path ./app.sqlite3
contextty inspect app-db
contextty snapshot app-db --profile-mode deep --row-limit 10000 --timeout 5s
contextty query "what tables explain signup state?" --source app-db --budget 2000
contextty serve --api
contextty serve --mcp
```

| Command | What it does |
| --- | --- |
| `contextty detect .` | Recursively scans the current project for likely database sources and verifies candidates before returning them. |
| `contextty source add app-db --type postgres --dsn-env DATABASE_URL` | Registers or updates a source named `app-db` using the connector and locator fields shown in the command. |
| `contextty source add local-db --type sqlite --path ./app.sqlite3` | Registers or updates a source named `local-db` using the connector and locator fields shown in the command. |
| `contextty source list` | Lists registered sources from the local Contextty store. |
| `contextty inspect app-db` | Connects to the registered source and returns schema metadata without writing a snapshot. |
| `contextty snapshot app-db --profile-mode deep --row-limit 10000 --timeout 5s` | Builds or refreshes the local graph artifact for `app-db` using deep profiling, up to 10,000 sampled rows, and a 5 second statement timeout. |
| `contextty query "what tables explain signup state?" --source app-db --budget 2000` | Queries the latest local artifact only, returning context for the question within a 2,000 word budget. |
| `contextty graph --source app-db` | Returns the latest local graph summary for a source. |
| `contextty serve --api` | Starts the local HTTP API server, defaulting to `127.0.0.1:8765`. |
| `contextty serve --mcp` | Starts the MCP stdio server so MCP clients can call Contextty tools. |

## Development

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e ".[dev]"
python3 -m pytest
```

## License

Contextty is licensed under the Apache License 2.0.
