# Contextty

Contextty v0.0.1 is a backend-only tool that turns one approved Postgres
source into a local AI-readable context artifact.

The first release is SQL-first, Postgres-only, and stores snapshots in
`.contextty/contextty.db`.

```bash
contextty detect .
contextty source add app-db --type postgres --dsn-env DATABASE_URL
contextty inspect app-db
contextty snapshot app-db --profile-mode deep --row-limit 10000 --timeout 5s
contextty query "what tables explain signup state?" --budget 2000
contextty serve --api
contextty serve --mcp
```

Snapshot reads are guarded by read-only Postgres session settings,
statement timeouts, bounded profiling queries, and local-only query APIs.
