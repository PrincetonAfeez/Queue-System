PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    leased_at TEXT,
    lease_expires_at TEXT,
    acked_at TEXT,
    dead_lettered_at TEXT,
    deleted_at TEXT,
    idempotency_key TEXT,
    last_error TEXT,
    version INTEGER NOT NULL DEFAULT 0,
    receipt_handle TEXT,
    worker_id TEXT,
    delivery_mode TEXT,
    redeliveries INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_message_id INTEGER NOT NULL REFERENCES messages(id),
    queue_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    failure_reason TEXT,
    attempts INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    dead_lettered_at TEXT NOT NULL,
    final_status TEXT NOT NULL,
    requeued_at TEXT,
    requeued_message_id INTEGER
);

CREATE TABLE IF NOT EXISTS queue_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    message_id INTEGER REFERENCES messages(id),
    worker_id TEXT,
    receipt_handle_short TEXT,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Idempotency dedup applies only to live (available/leased) messages so the
-- same key can be reused once its previous message reaches a terminal state.
-- The DROP keeps init_schema idempotent and migrates an older full-table index.
DROP INDEX IF EXISTS idx_messages_idempotency;
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency
    ON messages(queue_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND status IN ('available', 'leased');

CREATE INDEX IF NOT EXISTS idx_messages_claim
    ON messages(queue_name, status, available_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_messages_lease_expiry
    ON messages(status, lease_expires_at);

CREATE INDEX IF NOT EXISTS idx_messages_created
    ON messages(queue_name, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_receipt_handle
    ON messages(receipt_handle);

CREATE INDEX IF NOT EXISTS idx_dead_letters_queue
    ON dead_letters(queue_name, dead_lettered_at);

CREATE INDEX IF NOT EXISTS idx_queue_events_queue_created
    ON queue_events(queue_name, created_at);

CREATE INDEX IF NOT EXISTS idx_queue_events_type
    ON queue_events(queue_name, event_type, created_at);
