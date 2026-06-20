# Workers and Shutdown

## WorkerPool

`WorkerPool` runs one or more `Worker` threads that poll `Queue.dequeue()`,
invoke a `Processor`, and call `ack()` / `nack()` in at-least-once mode.

```python
from simplequeue import BackgroundSweeper, QueueConfig, ShutdownMode, WorkerPool, create_queue

config = QueueConfig(database_path="queue.db")
queue = create_queue(config, "jobs")
queue.init_schema()

def handle(delivery):
    return True

with WorkerPool(queue, handle, workers=4, shutdown_mode=ShutdownMode.FINISH_CURRENT) as pool:
    with BackgroundSweeper(queue, interval=1.0):
        ...
```

## Shutdown modes

| Mode | Stop before processor | Stop during processor | Stop after success |
|------|----------------------|----------------------|-------------------|
| `finish-current` | Run processor, then ack/nack | Finish, then ack/nack | Ack/nack normally |
| `nack-current` | Nack immediately | Finish, then nack | Nack (redelivery) |
| `abandon-current` | Leave lease | Leave lease on success path; no nack on exception during shutdown | Leave lease |

At-most-once deletes the message at claim time; shutdown modes only affect
whether the worker loop exits — there is nothing to ack/nack afterward. A
processor that returns ``False`` in at-most-once mode is logged as a warning
but cannot recover the message.

## At-least-once maintenance

Long-running worker pools do not reclaim expired leases automatically. Run
``BackgroundSweeper`` (at most one per SQLite database) or call ``Queue.sweep()``
periodically so crashed workers do not leave messages stuck in ``leased``.

``Queue.dequeue()`` also releases expired leases database-wide before each claim,
but that only happens while something is actively dequeuing.

## Join semantics

- `Worker.join(timeout)` → `bool`: `True` if the thread stopped.
- `WorkerPool.join(timeout)` → `bool`: `True` if **all** workers stopped.
- `BackgroundSweeper.join(timeout)` → `bool`: `True` if the sweeper thread stopped.

Context managers call `stop()` then `join(join_timeout)` (default 30 seconds).

Always call `join()` after `BackgroundSweeper.stop()` so the per-database
registry slot is released. A stale registry entry is cleared automatically when
starting a new sweeper if the previous thread has exited.

## CLI consume

`simplequeue consume` stops the pool and optional sweeper in a `finally` block
with a bounded join timeout (30 seconds). Ctrl-C and SIGTERM raise
`KeyboardInterrupt`, which triggers the same cleanup path.
