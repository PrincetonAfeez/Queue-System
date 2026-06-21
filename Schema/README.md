# Schema

This folder contains simple schema files for the Queue System project.

## Files

| File | Purpose |
| --- | --- |
| `schema.sql` | SQLite database tables and indexes for queue messages, dead letters, and queue events. |
| `config.schema.json` | JSON Schema for queue configuration files. |
| `message.schema.json` | Flexible JSON Schema for message payload validation. |
| `erd.mmd` | Mermaid ERD showing table relationships. |

## How to use the SQL schema

From the repository root:

```bash
sqlite3 queue.db < Schema/schema.sql
```

The project already includes an internal runtime schema under `src/simplequeue/storage/schema.sql`. This root-level `Schema/` folder is meant as a simple, easy-to-find reference and handoff folder.

## Core tables

- `messages`: stores queued jobs, delivery status, lease data, receipt handles, retry counts, and idempotency keys.
- `dead_letters`: stores jobs that exhausted retry attempts or were explicitly dead-lettered.
- `queue_events`: stores operational events for inspection and debugging.
- `schema_meta`: stores simple database metadata such as `schema_version`.
