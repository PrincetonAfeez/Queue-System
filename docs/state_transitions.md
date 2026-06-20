# State Transitions

Valid transitions:

```text
available -> leased
leased -> acked
leased -> available          # nack retry or lease expiry
leased -> dead_lettered      # max attempts exhausted
available -> dead_lettered   # defensive: already-exhausted message never delivered
dead_lettered -> available   # explicit DLQ requeue
available -> deleted         # at-most-once delivery
```

Invalid transitions:

```text
acked -> leased
deleted -> leased
dead_lettered -> leased without explicit requeue
leased by old receipt -> acked
leased by old receipt -> available
```

`SQLiteBackend` enforces the operational transitions with guarded updates, and
also calls `simplequeue.core.states.assert_legal_transition` at each mutation so
the documented state machine is executable, not just descriptive. The same
module's `LEGAL_TRANSITIONS` table is what the unit tests assert against.

There is no `expired` message status: message-TTL eviction is out of scope. The
`expired` figure in the stats snapshot counts expired *leases* (a lease deadline
passing), which is a separate concept.

DLQ requeue resets the original message to `available`, sets `attempts` to `0`,
clears lease fields and `redeliveries`, records a `requeue` event, and updates
the historical DLQ row with `requeued_at`.
