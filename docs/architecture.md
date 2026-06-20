# Architecture

The queue is split into three layers:

1. `simplequeue.core.Queue` is the public API. Producers, consumers, workers,
   demos, and the CLI call this layer.
2. `simplequeue.storage.StorageBackend` is the persistence contract. The queue
   API delegates state changes to the backend instead of embedding SQL.
3. `simplequeue.storage.SQLiteBackend` owns SQLite schema, transactions,
   guarded updates, event records, and row mapping.

This separation keeps queue behavior stable if another backend is added later.
The CLI has no delivery correctness logic; it constructs a queue and calls the
same methods a Python application would call. `simplequeue.cli.main` builds the
argparse parser and dispatches; each subcommand lives in its own module under
`simplequeue.cli.commands/` (with shared helpers in `simplequeue.cli._shared`),
and every handler is a thin shell over the `Queue` API.

Library-wide defaults live in one place (`simplequeue.defaults`) so the Python
API, the worker classes, and `QueueConfig` cannot drift to different values for
the same setting. Reliability rules (retry decisions, lease validity, receipt
minting, DLQ reasons) live in `simplequeue.reliability` and are shared by the
backend and the tests rather than re-inlined as literals.

## Message Lifecycle

Messages move through explicit statuses:

```text
available -> leased
leased -> acked
leased -> available
leased -> dead_lettered
available -> dead_lettered
available -> deleted
dead_lettered -> available
```

Illegal transitions, such as `acked -> leased` or stale receipt mutation, fail
without changing state. `simplequeue.core.states.assert_legal_transition` makes
the table executable at every backend mutation.

## Time Model

The library stores durable deadlines as UTC timestamps so leases remain
meaningful after process restart. In-process loops use the injectable `Clock`
interface so tests can use `FakeClock` without real sleeps.

## Observability

Every mutation records a row in `queue_events`. Event-type names are defined
once in `simplequeue.observability.events` and referenced by the backend, the
queue API, and the workers so written events and stats keys cannot diverge.
Stats are computed from backend state and event counts using the injected clock
for the "recent" window, then optionally cached by `StatsCache`. Structured log
messages mirror important lifecycle events for demos and local debugging.
