# Delivery Guarantees

## At-Most-Once

At-most-once mode marks a message `deleted` during delivery. Processing happens
after the terminal state change. If the worker dies, there is no later ack step
that can recover the work. A later `ack()` or `nack()` on the same receipt
returns `not_leased` because the message is already terminal.

Use this for low-value work where speed and no duplicates matter more than loss.

If a worker processor returns ``False`` in at-most-once mode, the message is
already deleted and cannot be nacked or retried; the worker logs a warning.

```powershell
simplequeue demo --db demo.db at-most-once-loss
```

## At-Least-Once

At-least-once mode marks a message `leased` during delivery. The message becomes
`acked` only after a valid `ack(receipt_handle)`. If the worker dies before ack,
the lease expires and `sweep()` returns the message to `available`, unless the
attempt limit has been exhausted and the message must go to DLQ.

Long-running CLI consumers need `--sweeper` or periodic `simplequeue sweep` to
reclaim expired leases automatically. Without sweeping, leased messages stay
in-flight until something calls `Queue.sweep()` or a consumer dequeues (each
dequeue pass releases expired leases database-wide before claiming).

Use this for valuable work, and make consumers idempotent. A worker may process
the same message more than once if it crashes after the side effect but before
ack.

```powershell
simplequeue demo --db demo.db at-least-once-redelivery
```

## Receipt Handles

The receipt handle is a lease token. Ack and nack validate:

- the receipt exists on the message
- the message is still leased
- the lease has not expired
- the storage update changes exactly one row

This prevents a stale worker from mutating a message after another worker owns
a newer lease.
