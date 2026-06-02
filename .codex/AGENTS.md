# Connector Implementation Guide

Contextty treats every database type as a separate connector. Do not mix one
database's connection, introspection, SQL dialect, or profiling logic into
another connector module.

## Required Connector Shape

- Add a dedicated module under `src/contextty/connectors/`, for example
  `postgres.py` or `sqlite.py`.
- Provide a connector class that opens read-only connections with bounded
  connection/query behavior.
- Provide an introspector class that returns the shared model types from
  `src/contextty/models.py`: tables, columns, primary keys, foreign keys,
  indexes, views, inspection results, and table profiles.
- Keep dialect-specific SQL and metadata queries inside that connector module.
- Use structured driver APIs and parameter binding. Do not parse database
  metadata with ad hoc string scraping when the driver or catalog exposes a
  structured API.

## Read-Only And Bounds

Every connector must default to read-only operation:

- Use driver or server settings that reject writes.
- Validate user-provided SQL with the shared read-only guard before execution.
- Bound connection waits and query execution time.
- Bound profiling with `SnapshotOptions.row_limit`.
- Avoid APIs that mutate session, schema, data, or server state.

## Source Registration

Source storage is connector-neutral, with connector-specific locator fields:

- Postgres uses `connector_type = "postgres"` and `dsn_env`.
- SQLite uses `connector_type = "sqlite"` and `path`.
- New database types must add their own nullable locator field only when needed,
  update validation in `LocalStore.add_source`, and keep unrelated locator
  fields empty.

Fresh local stores only need the current schema. This project does not require
backward-compatible migrations for discarded local `.contextty/contextty.db`
stores unless a task explicitly asks for them.

## Detection

Update `src/contextty/detect.py` for each new connector:

- Recursively detect sources under the requested path.
- Verify candidates before returning them. For example, SQLite files must open
  successfully through a read-only SQLite connection.
- Return `connector_type`, a default `name`, `confidence`, `source`, and only
  the locator fields relevant to that connector.

## CLI, API, And MCP

Every connector must be exposed consistently:

- CLI registration: `contextty source add NAME --type TYPE` plus that
  connector's locator fields.
- API registration: `POST /v1/sources` with `type` plus connector-specific
  fields.
- MCP registration: `detect_sources` followed by `add_source`, then
  `inspect_source` or `refresh_snapshot`, then `query_context`.
- Interactive CLI detection may offer to register detected sources. Non-
  interactive detection must remain JSON-only.

## Service Dispatch

`src/contextty/services.py` dispatches by `source.connector_type`. New connector
types must be added there so inspect and snapshot refresh use the matching
connector and introspector.

Snapshot source nodes must stay connector-neutral. Include
`connector_type` and only the locator field for the active connector.

## Generic Context Algorithms

Contextty is intended to work for any database content. Retrieval, ranking,
fact derivation, benchmark assistance, and MCP responses must stay generic:

- Do not hardcode table names, column names, row values, business-domain terms,
  benchmark question text, or expected answers into Contextty core logic.
- Prefer algorithms that derive routing clues dynamically from the active
  snapshot: table names, column names, primary keys, foreign keys, indexes,
  value profiles, row-count/profile facts, and bounded derived facts.
- If the LLM needs help, provide compact dynamic hints from the linked database,
  such as likely tables, likely columns, FK paths, matched schema terms, and
  answer-ready facts. Do not provide hand-authored clues for a specific fixture.
- Generic database-intent vocabulary is acceptable only when it is independent
  of database content, for example terms for schema, rows, columns, keys,
  indexes, counts, averages, sums, minimums, maximums, and ordering.
- Tests and benchmarks may contain fixture-specific questions and expected
  answers, but production retrieval behavior must not depend on those fixture
  strings.

## Tests And Docs

For every new database type, add focused tests for:

- Connector introspection, including tables, views, columns, primary keys,
  foreign keys, indexes, and view definitions.
- Read-only write rejection.
- Basic and deep profiling behavior.
- CLI, API, and MCP source registration.
- Snapshot refresh and local-only `query_context`.
- Recursive detection, including rejected false positives.
- Fresh-store source schema fields.

Update user-facing docs when supported connector behavior changes.
