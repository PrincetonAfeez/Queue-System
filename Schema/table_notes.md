# Queue System Table Notes

## messages

Primary queue table. Each row represents a message/job. Important fields:

- `queue_name`: logical queue name.
- `payload`: JSON payload stored as text.
- `status`: message lifecycle state, such as `available`, `leased`, `acked`, `dead_lettered`, or `deleted`.
- `receipt_handle`: unique token used to ack/nack leased messages safely.
- `version`: optimistic guard used to prevent stale updates.
- `idempotency_key`: optional dedupe key for live messages.

## dead_letters

Stores failed messages after retries are exhausted. Keeps payload, attempts, failure reason, and original message link.

## queue_events

Append-style event log for operational visibility. Useful for debugging enqueue, dequeue, ack, nack, sweep, DLQ, and requeue behavior.

## Important indexes

- `idx_messages_claim`: supports finding claimable messages quickly.
- `idx_messages_lease_expiry`: supports sweeping expired leases.
- `idx_messages_idempotency`: prevents duplicate live idempotency keys per queue.
