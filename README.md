# Contextty

Give AI agents a local map of your database without giving them the database.

Contextty turns approved PostgreSQL and SQLite sources into local,
AI-readable context artifacts. It captures schema metadata, relationship
graphs, and bounded profiling summaries so agents can answer database
structure questions through the CLI, HTTP API, or MCP without querying the live
database during normal context lookup.

## Why Contextty

- Local snapshots: build a `.contextty/contextty.db` artifact that agents can
  query without live database access.
- Connector-based sources: keep PostgreSQL and SQLite connection,
  introspection, SQL dialect, and profiling behavior isolated by connector.
- Read-only, bounded access: inspect and snapshot operations use read-only
  connections, SQL guardrails, statement timeouts, and row limits.

## Quickstart

Install Contextty:

```bash
pip install contextty
```

Detect database sources in a project:

```bash
contextty detect .
```

Register an approved PostgreSQL source whose DSN is stored in an environment
variable:

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/app'
contextty source add app-db --type postgres --dsn-env DATABASE_URL
```

Register a local SQLite database file:

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

Contextty snapshots are database context, not database backups. A snapshot can
include:

- Schemas, tables, views, and view definitions.
- Columns, data types, nullability, and defaults.
- Primary keys, foreign keys, indexes, and relationship edges.
- Database, schema, table, view, column, index, and relationship graph nodes.
- Context pills that summarize useful local facts for AI agents.
- Basic profiling summaries, with optional deep profiling over a bounded row
  sample.

After a snapshot is built, `contextty query` searches the local graph artifact,
ranks matching nodes, follows nearby graph edges, and renders context within
the requested word budget.

## Supported Sources

| Database source | Status | Registration | Notes |
| --- | --- | --- | --- |
| PostgreSQL | Available now | `--type postgres --dsn-env DATABASE_URL` | Connects through a DSN stored in an environment variable. |
| SQLite | Available now | `--type sqlite --path ./app.sqlite3` | Connects to a local SQLite database file. |
| Future connectors | Connector model ready | Connector-specific fields | New database types should live in dedicated connector modules with isolated dialect logic. |

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
- Connectors default to read-only database access.
- User-provided SQL goes through the shared read-only guard before execution.
- Connection waits and query execution are bounded with timeouts.
- Profiling is bounded by `SnapshotOptions.row_limit`.
- `contextty query` reads the local snapshot only; it does not execute SQL
  against the live database.

## Command Reference

These commands cover the common PostgreSQL and SQLite workflows:

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
| `contextty detect .` | Recursively scans the current project for likely PostgreSQL connection configuration and verified SQLite database files. |
| `contextty source add app-db --type postgres --dsn-env DATABASE_URL` | Registers or updates a source named `app-db` that uses the PostgreSQL connector and reads its DSN from `DATABASE_URL`. |
| `contextty source add local-db --type sqlite --path ./app.sqlite3` | Registers or updates a source named `local-db` that uses the SQLite connector and reads `./app.sqlite3` directly. |
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
