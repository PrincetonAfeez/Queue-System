# Final Demo Script

Run these commands from a fresh virtual environment after `pip install -e ".[dev]"`.

## Demo 1: Basic Queue

```powershell
simplequeue demo --db demo.db basic
```

Shows initialization, enqueue, consume, ack, and stats.

## Demo 2: Concurrent Workers

```powershell
simplequeue demo --db demo.db concurrent-workers
```

Starts multiple local worker threads and proves there are no duplicate message
ids in the processed set.

## Demo 3: At-Most-Once Loss

```powershell
simplequeue demo --db demo.db at-most-once-loss
```

Shows delete-on-delivery behavior. The message is terminal after delivery and
cannot be redelivered after a worker crash.

## Demo 4: At-Least-Once Redelivery

```powershell
simplequeue demo --db demo.db at-least-once-redelivery
```

Uses `FakeClock` to expire the lease without sleeping, sweeps, and shows the
same message delivered again with a new receipt handle.

## Demo 5: Retry And DLQ

```powershell
simplequeue demo --db demo.db retry-dlq
```

Forces failures until max attempts are exhausted, moves the message to DLQ, and
then requeues it.

## Demo 6: Receipt Handles

```powershell
simplequeue demo --db demo.db receipt-handle-stale-ack
```

Shows an old receipt failing after the message is leased again, while the fresh
receipt can ack successfully.

## Demo 7: Cache And Observability

Run after Demo 1 (or any earlier demo) against the **same** `--db demo.db` so
the queue already contains data:

```powershell
simplequeue stats --db demo.db --queue demo
simplequeue stats --db demo.db --queue demo --no-cache
```

Stats can be cached, but queue delivery correctness still comes from SQLite.

## Demo 8: Unsafe Designs

```powershell
simplequeue demo unsafe-double-claim
simplequeue demo unsafe-stale-ack
simplequeue demo unsafe-no-visibility-timeout
simplequeue demo unsafe-cache-correctness
```

These are teaching-only examples that intentionally demonstrate the failure
modes the production design avoids. Unsafe demos always ignore `--db` and run in
isolated in-memory or temporary teaching databases.

## Demo 9: Run All Safe Demos

```powershell
simplequeue demo --db demo.db all
```

Runs every safe demo sequentially against the same database file.
