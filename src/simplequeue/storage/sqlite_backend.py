""" SQLite backend. """

from __future__ import annotations

import functools
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from simplequeue.core.delivery import Delivery
from simplequeue.core.exceptions import DeadLetterNotFound, IdempotencyConflict, StorageError
from simplequeue.core.message import DeadLetter, Message, MessageDetails, QueueEvent
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.results import AckResult, ClaimResult, LeaseReleaseResult, NackResult
from simplequeue.core.states import MessageStatus, assert_legal_transition
from simplequeue.core.validation import validate_queue_name
from simplequeue.observability import events
from simplequeue.observability.stats import QueueStatsSnapshot
from simplequeue.reliability.dlq import DLQ_REASON_LEASE_EXPIRED, DLQ_REASON_MAX_ATTEMPTS
from simplequeue.reliability.leases import lease_is_active
from simplequeue.reliability.receipt_handles import new_receipt_handle
from simplequeue.reliability.retry import decide_retry
from simplequeue.storage.base import StorageBackend
from simplequeue.storage.migrations import apply_schema_version, load_schema_sql
from simplequeue.storage.serializers import dumps, loads

# Bounded retries on idempotency-key races; see docs/migrations.md for concurrency notes.
IDEMPOTENCY_ENQUEUE_MAX_RETRIES = 25

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _wrap_storage_errors(method: Callable[_P, _R]) -> Callable[_P, _R]:
    """Surface low-level ``sqlite3.Error`` as a domain ``StorageError``.

    Turns cryptic failures (for example using the queue before ``init_schema``
    runs: ``no such table: messages``) into a clear, catchable error that the
    CLI maps to a useful exit code. Domain exceptions such as
    ``DeadLetterNotFound`` are not sqlite errors and pass through unchanged.
    """

    @functools.wraps(method)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return method(*args, **kwargs)
        except sqlite3.Error as error:
            raise StorageError(f"storage operation {method.__name__!r} failed: {error}") from error

    return wrapper


def _dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    # Canonical fixed-width UTC form so lexicographic order == chronological
    # order regardless of the source datetime's precision or timezone.
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _short(receipt_handle: str | None) -> str | None:
    return receipt_handle[:12] if receipt_handle else None


class SQLiteBackend(StorageBackend):
    """SQLite implementation with race-safe guarded claims.

    SQLite has database-level write locks rather than Postgres-style row locks.
    Each mutation below uses a short ``BEGIN IMMEDIATE`` transaction plus a
    guarded ``UPDATE`` so concurrent consumers cannot both win the same claim.
    Every connection is opened through ``contextlib.closing`` so the database
    file (and its WAL sidecar files) are released promptly instead of lingering
    until garbage collection.
    """

    def __init__(self, db_path: str | Path, default_max_attempts: int = 3) -> None:
        if default_max_attempts < 1:
            raise ValueError("default_max_attempts must be >= 1")
        self.db_path = Path(db_path)
        self.default_max_attempts = default_max_attempts

    def _connect(self) -> sqlite3.Connection:
        # One short-lived connection per operation, always closed via
        # contextlib.closing at the call site. This keeps the model simple and
        # portable (no lingering open handles on the database file); a pooled or
        # thread-local connection would be the optimization if throughput mattered.
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    @_wrap_storage_errors
    def init_schema(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(load_schema_sql())
            conn.execute("BEGIN IMMEDIATE")
            try:
                apply_schema_version(conn)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def enqueue(
        self,
        queue_name: str,
        payload: Any,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        max_attempts: int | None = None,
        now: datetime | None = None,
    ) -> int:
        queue_name = validate_queue_name(queue_name)
        # Use the caller's clock so created_at/event timestamps stay in the same
        # clock domain as lease deadlines (important under FakeClock).
        now = now or datetime.now(UTC)
        available = available_at or now
        resolved_max_attempts = (
            max_attempts if max_attempts is not None else self.default_max_attempts
        )
        payload_json = dumps(payload)
        insert_sql = """
                        INSERT INTO messages (
                            queue_name, payload, status, attempts, max_attempts,
                            created_at, updated_at, available_at, idempotency_key,
                            version
                        )
                        VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, 0)
                        """
        idempotent_insert_sql = insert_sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        with closing(self._connect()) as conn:
            attempts = IDEMPOTENCY_ENQUEUE_MAX_RETRIES if idempotency_key else 1
            for _ in range(attempts):
                conn.execute("BEGIN IMMEDIATE")
                try:
                    cur = conn.execute(
                        idempotent_insert_sql if idempotency_key else insert_sql,
                        (
                            queue_name,
                            payload_json,
                            MessageStatus.AVAILABLE.value,
                            resolved_max_attempts,
                            _dt(now),
                            _dt(now),
                            _dt(available),
                            idempotency_key,
                        ),
                    )
                    if idempotency_key is None:
                        if cur.lastrowid is None:
                            raise RuntimeError("SQLite did not return a message id for the insert")
                        message_id = int(cur.lastrowid)
                        self._record_event(
                            conn,
                            events.ENQUEUE,
                            queue_name,
                            message_id,
                            None,
                            None,
                            {"idempotency_key": idempotency_key},
                            now,
                        )
                        conn.commit()
                        return message_id

                    if cur.rowcount > 0 and cur.lastrowid:
                        message_id = int(cur.lastrowid)
                        self._record_event(
                            conn,
                            events.ENQUEUE,
                            queue_name,
                            message_id,
                            None,
                            None,
                            {"idempotency_key": idempotency_key},
                            now,
                        )
                        conn.commit()
                        return message_id

                    row = conn.execute(
                        """
                        SELECT id, payload FROM messages
                        WHERE queue_name = ?
                          AND idempotency_key = ?
                          AND status IN (?, ?)
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (
                            queue_name,
                            idempotency_key,
                            MessageStatus.AVAILABLE.value,
                            MessageStatus.LEASED.value,
                        ),
                    ).fetchone()
                    if row is None:
                        conn.rollback()
                        continue
                    conn.commit()
                    stored_payload = dumps(loads(str(row["payload"])))
                    if stored_payload != payload_json:
                        raise IdempotencyConflict(
                            f"idempotency key {idempotency_key!r} is already in use with a different payload"
                        )
                    return int(row["id"])
                except sqlite3.IntegrityError:
                    if idempotency_key is None:
                        conn.rollback()
                        raise
                    conn.rollback()
                    continue
                except BaseException:
                    conn.rollback()
                    raise
            raise StorageError(
                f"failed to enqueue message with idempotency key {idempotency_key!r} after retries"
            )

    @_wrap_storage_errors
    def claim_next(
        self,
        queue_name: str,
        delivery_mode: DeliveryMode,
        visibility_timeout: timedelta,
        worker_id: str,
        now: datetime,
    ) -> ClaimResult:
        queue_name = validate_queue_name(queue_name)
        mode = DeliveryMode.parse(delivery_mode)
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                lease_release = self._release_expired_leases_locked(conn, now)
                mutated = lease_release.total > 0
                while True:
                    row = conn.execute(
                        """
                        SELECT * FROM messages
                        WHERE queue_name = ?
                          AND status = ?
                          AND available_at <= ?
                          AND attempts < max_attempts
                        ORDER BY created_at ASC, id ASC
                        LIMIT 1
                        """,
                        (queue_name, MessageStatus.AVAILABLE.value, _dt(now)),
                    ).fetchone()
                    if row is None:
                        conn.commit()
                        return ClaimResult(None, mutated)

                    receipt_handle = new_receipt_handle()
                    attempt = int(row["attempts"]) + 1
                    message_id = int(row["id"])
                    if mode is DeliveryMode.AT_MOST_ONCE:
                        assert_legal_transition(MessageStatus.AVAILABLE, MessageStatus.DELETED)
                        cur = conn.execute(
                            """
                            UPDATE messages
                            SET status = ?, attempts = ?, updated_at = ?,
                                deleted_at = ?, receipt_handle = ?,
                                worker_id = ?, delivery_mode = ?,
                                version = version + 1
                            WHERE id = ? AND status = ? AND version = ?
                            """,
                            (
                                MessageStatus.DELETED.value,
                                attempt,
                                _dt(now),
                                _dt(now),
                                receipt_handle,
                                worker_id,
                                mode.value,
                                message_id,
                                MessageStatus.AVAILABLE.value,
                                int(row["version"]),
                            ),
                        )
                        if cur.rowcount != 1:
                            continue
                        self._record_event(
                            conn,
                            events.DELETE_DELIVERY,
                            queue_name,
                            message_id,
                            worker_id,
                            receipt_handle,
                            {"attempt": attempt, "mode": mode.value},
                            now,
                        )
                        conn.commit()
                        return ClaimResult(
                            Delivery(
                                message_id=message_id,
                                receipt_handle=receipt_handle,
                                queue_name=queue_name,
                                payload=loads(str(row["payload"])),
                                attempt=attempt,
                                delivery_mode=mode,
                                leased_at=now,
                                lease_expires_at=None,
                            ),
                            True,
                        )

                    assert_legal_transition(MessageStatus.AVAILABLE, MessageStatus.LEASED)
                    lease_expires_at = now + visibility_timeout
                    cur = conn.execute(
                        """
                        UPDATE messages
                        SET status = ?, attempts = ?, leased_at = ?,
                            lease_expires_at = ?, updated_at = ?,
                            receipt_handle = ?, worker_id = ?,
                            delivery_mode = ?, version = version + 1
                        WHERE id = ? AND status = ? AND version = ?
                        """,
                        (
                            MessageStatus.LEASED.value,
                            attempt,
                            _dt(now),
                            _dt(lease_expires_at),
                            _dt(now),
                            receipt_handle,
                            worker_id,
                            mode.value,
                            message_id,
                            MessageStatus.AVAILABLE.value,
                            int(row["version"]),
                        ),
                    )
                    if cur.rowcount != 1:
                        continue
                    self._record_event(
                        conn,
                        events.LEASE,
                        queue_name,
                        message_id,
                        worker_id,
                        receipt_handle,
                        {"attempt": attempt, "mode": mode.value},
                        now,
                    )
                    if attempt > 1:
                        self._record_event(
                            conn,
                            events.REDELIVER,
                            queue_name,
                            message_id,
                            worker_id,
                            receipt_handle,
                            {"attempt": attempt},
                            now,
                        )
                    conn.commit()
                    return ClaimResult(
                        Delivery(
                            message_id=message_id,
                            receipt_handle=receipt_handle,
                            queue_name=queue_name,
                            payload=loads(str(row["payload"])),
                            attempt=attempt,
                            delivery_mode=mode,
                            leased_at=now,
                            lease_expires_at=lease_expires_at,
                        ),
                        True,
                    )
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def ack(self, receipt_handle: str, now: datetime) -> AckResult:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM messages WHERE receipt_handle = ?",
                    (receipt_handle,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return AckResult(False, None, "invalid", "receipt_handle_not_found")
                message_id = int(row["id"])
                if row["status"] != MessageStatus.LEASED.value:
                    conn.commit()
                    return AckResult(False, message_id, str(row["status"]), "not_leased")
                lease_expires_at = _parse_dt(row["lease_expires_at"])
                if not lease_is_active(lease_expires_at, now):
                    conn.commit()
                    return AckResult(False, message_id, MessageStatus.LEASED.value, "lease_expired")

                assert_legal_transition(MessageStatus.LEASED, MessageStatus.ACKED)
                cur = conn.execute(
                    """
                    UPDATE messages
                    SET status = ?, acked_at = ?, updated_at = ?,
                        receipt_handle = NULL, worker_id = NULL,
                        lease_expires_at = NULL, leased_at = NULL,
                        version = version + 1
                    WHERE receipt_handle = ?
                      AND status = ?
                      AND lease_expires_at > ?
                    """,
                    (
                        MessageStatus.ACKED.value,
                        _dt(now),
                        _dt(now),
                        receipt_handle,
                        MessageStatus.LEASED.value,
                        _dt(now),
                    ),
                )
                if cur.rowcount != 1:
                    conn.commit()
                    return AckResult(False, message_id, MessageStatus.LEASED.value, "stale_receipt")
                self._record_event(
                    conn,
                    events.ACK,
                    str(row["queue_name"]),
                    message_id,
                    row["worker_id"],
                    receipt_handle,
                    {"attempt": int(row["attempts"])},
                    now,
                )
                conn.commit()
                return AckResult(True, message_id, MessageStatus.ACKED.value)
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def nack(
        self,
        receipt_handle: str,
        now: datetime,
        reason: str | None = None,
    ) -> NackResult:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM messages WHERE receipt_handle = ?",
                    (receipt_handle,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return NackResult(False, None, "invalid", "receipt_handle_not_found")
                message_id = int(row["id"])
                if row["status"] != MessageStatus.LEASED.value:
                    conn.commit()
                    return NackResult(False, message_id, str(row["status"]), "not_leased")
                lease_expires_at = _parse_dt(row["lease_expires_at"])
                if not lease_is_active(lease_expires_at, now):
                    return self._nack_expired_lease_locked(conn, row, now, reason)

                if decide_retry(int(row["attempts"]), int(row["max_attempts"])).should_dead_letter:
                    if not self._move_to_dlq_locked(
                        conn,
                        row,
                        reason or DLQ_REASON_MAX_ATTEMPTS,
                        now,
                    ):
                        conn.commit()
                        return NackResult(
                            False,
                            message_id,
                            MessageStatus.LEASED.value,
                            "stale_receipt",
                        )
                    self._record_event(
                        conn,
                        events.NACK,
                        str(row["queue_name"]),
                        message_id,
                        row["worker_id"],
                        receipt_handle,
                        {"attempt": int(row["attempts"]), "reason": reason, "moved_to_dlq": True},
                        now,
                    )
                    conn.commit()
                    return NackResult(
                        True,
                        message_id,
                        MessageStatus.DEAD_LETTERED.value,
                        reason,
                        moved_to_dlq=True,
                    )

                assert_legal_transition(MessageStatus.LEASED, MessageStatus.AVAILABLE)
                cur = conn.execute(
                    """
                    UPDATE messages
                    SET status = ?, available_at = ?, updated_at = ?,
                        leased_at = NULL, lease_expires_at = NULL,
                        receipt_handle = NULL, worker_id = NULL,
                        last_error = ?, redeliveries = redeliveries + 1,
                        version = version + 1
                    WHERE receipt_handle = ?
                      AND status = ?
                      AND lease_expires_at > ?
                    """,
                    (
                        MessageStatus.AVAILABLE.value,
                        _dt(now),
                        _dt(now),
                        reason,
                        receipt_handle,
                        MessageStatus.LEASED.value,
                        _dt(now),
                    ),
                )
                if cur.rowcount != 1:
                    conn.commit()
                    return NackResult(False, message_id, MessageStatus.LEASED.value, "stale_receipt")
                self._record_event(
                    conn,
                    events.NACK,
                    str(row["queue_name"]),
                    message_id,
                    row["worker_id"],
                    receipt_handle,
                    {"attempt": int(row["attempts"]), "reason": reason},
                    now,
                )
                conn.commit()
                return NackResult(True, message_id, MessageStatus.AVAILABLE.value, reason)
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def release_expired_leases(self, now: datetime) -> LeaseReleaseResult:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._release_expired_leases_locked(conn, now)
                conn.commit()
                return result
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def move_exhausted_to_dlq(self, now: datetime) -> int:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    """
                    SELECT * FROM messages
                    WHERE status = ? AND attempts >= max_attempts
                    """,
                    (MessageStatus.AVAILABLE.value,),
                ).fetchall()
                dead_lettered = 0
                for row in rows:
                    if self._move_to_dlq_locked(conn, row, DLQ_REASON_MAX_ATTEMPTS, now):
                        dead_lettered += 1
                conn.commit()
                return dead_lettered
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def requeue_dead_letter(self, message_id: int, queue_name: str, now: datetime) -> int:
        queue_name = validate_queue_name(queue_name)
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    """
                    SELECT * FROM messages
                    WHERE id = ? AND queue_name = ? AND status = ?
                    """,
                    (message_id, queue_name, MessageStatus.DEAD_LETTERED.value),
                ).fetchone()
                if row is None:
                    conn.commit()
                    raise DeadLetterNotFound(
                        f"dead-lettered message {message_id} was not found in queue {queue_name!r}"
                    )
                idempotency_key = row["idempotency_key"]
                if idempotency_key is not None:
                    conflict = conn.execute(
                        """
                        SELECT id FROM messages
                        WHERE queue_name = ?
                          AND idempotency_key = ?
                          AND id != ?
                          AND status IN (?, ?)
                        LIMIT 1
                        """,
                        (
                            queue_name,
                            idempotency_key,
                            message_id,
                            MessageStatus.AVAILABLE.value,
                            MessageStatus.LEASED.value,
                        ),
                    ).fetchone()
                    if conflict is not None:
                        conn.commit()
                        raise IdempotencyConflict(
                            f"cannot requeue message {message_id}: idempotency key "
                            f"{idempotency_key!r} is already held by live message "
                            f"{int(conflict['id'])}"
                        )
                assert_legal_transition(MessageStatus.DEAD_LETTERED, MessageStatus.AVAILABLE)
                conn.execute(
                    """
                    UPDATE messages
                    SET status = ?, attempts = 0, available_at = ?,
                        updated_at = ?, leased_at = NULL,
                        lease_expires_at = NULL, receipt_handle = NULL,
                        worker_id = NULL, dead_lettered_at = NULL,
                        last_error = NULL, redeliveries = 0,
                        version = version + 1
                    WHERE id = ? AND status = ?
                    """,
                    (
                        MessageStatus.AVAILABLE.value,
                        _dt(now),
                        _dt(now),
                        message_id,
                        MessageStatus.DEAD_LETTERED.value,
                    ),
                )
                conn.execute(
                    """
                    UPDATE dead_letters
                    SET requeued_at = ?, requeued_message_id = ?
                    WHERE original_message_id = ?
                      AND requeued_at IS NULL
                    """,
                    (_dt(now), message_id, message_id),
                )
                self._record_event(
                    conn,
                    events.REQUEUE,
                    str(row["queue_name"]),
                    message_id,
                    None,
                    None,
                    {"source": "dlq"},
                    now,
                )
                conn.commit()
                return message_id
            except BaseException:
                conn.rollback()
                raise

    @_wrap_storage_errors
    def peek(self, queue_name: str, limit: int = 10, now: datetime | None = None) -> list[Message]:
        queue_name = validate_queue_name(queue_name)
        reference = now or datetime.now(UTC)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT * FROM messages
                WHERE queue_name = ?
                  AND status = ?
                  AND available_at <= ?
                  AND attempts < max_attempts
                ORDER BY created_at ASC, id ASC
                LIMIT ?
                """,
                (queue_name, MessageStatus.AVAILABLE.value, _dt(reference), limit),
            ).fetchall()
            return [self._message_from_row(row) for row in rows]

    @_wrap_storage_errors
    def inspect(self, message_id: int) -> MessageDetails | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
            if row is None:
                return None
            events_rows = conn.execute(
                """
                SELECT * FROM queue_events
                WHERE message_id = ?
                ORDER BY created_at ASC, id ASC
                """,
                (message_id,),
            ).fetchall()
            dead = conn.execute(
                """
                SELECT * FROM dead_letters
                WHERE original_message_id = ?
                ORDER BY dead_lettered_at DESC, id DESC
                LIMIT 1
                """,
                (message_id,),
            ).fetchone()
            return MessageDetails(
                message=self._message_from_row(row),
                events=[self._event_from_row(event) for event in events_rows],
                dead_letter=self._dead_letter_from_row(dead) if dead is not None else None,
            )

    @_wrap_storage_errors
    def stats(self, queue_name: str, now: datetime | None = None) -> QueueStatsSnapshot:
        queue_name = validate_queue_name(queue_name)
        reference = now or datetime.now(UTC)
        with closing(self._connect()) as conn:
            event_counts = {
                str(row["event_type"]): int(row["count"])
                for row in conn.execute(
                    """
                    SELECT event_type, COUNT(*) AS count
                    FROM queue_events
                    WHERE queue_name = ?
                    GROUP BY event_type
                    """,
                    (queue_name,),
                ).fetchall()
            }
            depth = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM messages
                    WHERE queue_name = ?
                      AND status = ?
                      AND available_at <= ?
                      AND attempts < max_attempts
                    """,
                    (queue_name, MessageStatus.AVAILABLE.value, _dt(reference)),
                ).fetchone()["count"]
            )
            scheduled_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM messages
                    WHERE queue_name = ?
                      AND status = ?
                      AND available_at > ?
                    """,
                    (queue_name, MessageStatus.AVAILABLE.value, _dt(reference)),
                ).fetchone()["count"]
            )
            in_flight = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM messages
                    WHERE queue_name = ? AND status = ?
                    """,
                    (queue_name, MessageStatus.LEASED.value),
                ).fetchone()["count"]
            )
            recent_cutoff = _dt(reference - timedelta(seconds=60))
            recent_acked = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM queue_events
                    WHERE queue_name = ?
                      AND event_type = ?
                      AND created_at >= ?
                    """,
                    (queue_name, events.ACK, recent_cutoff),
                ).fetchone()["count"]
            )
            recent_worker_ids = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT worker_id) AS count
                    FROM queue_events
                    WHERE queue_name = ?
                      AND worker_id IS NOT NULL
                      AND created_at >= ?
                    """,
                    (queue_name, recent_cutoff),
                ).fetchone()["count"]
            )
            return QueueStatsSnapshot(
                queue_name=queue_name,
                enqueued=event_counts.get(events.ENQUEUE, 0),
                delivered=event_counts.get(events.LEASE, 0)
                + event_counts.get(events.DELETE_DELIVERY, 0),
                acked=event_counts.get(events.ACK, 0),
                nacked=event_counts.get(events.NACK, 0),
                redelivered=event_counts.get(events.REDELIVER, 0),
                dead_lettered=event_counts.get(events.DEAD_LETTER, 0),
                expired=event_counts.get(events.LEASE_EXPIRED, 0),
                current_depth=depth,
                scheduled_count=scheduled_count,
                in_flight_count=in_flight,
                recent_worker_ids=recent_worker_ids,
                recent_throughput=recent_acked / 60.0,
            )

    @_wrap_storage_errors
    def list_queues(self) -> list[str]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT queue_name FROM messages
                UNION
                SELECT queue_name FROM queue_events
                UNION
                SELECT queue_name FROM dead_letters
                ORDER BY queue_name
                """
            ).fetchall()
            return [str(row["queue_name"]) for row in rows]

    @_wrap_storage_errors
    def list_dead_letters(self, queue_name: str | None = None) -> list[DeadLetter]:
        if queue_name is not None:
            queue_name = validate_queue_name(queue_name)
        with closing(self._connect()) as conn:
            if queue_name is None:
                rows = conn.execute(
                    """
                    SELECT * FROM dead_letters
                    WHERE requeued_at IS NULL
                    ORDER BY dead_lettered_at DESC, id DESC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM dead_letters
                    WHERE queue_name = ?
                      AND requeued_at IS NULL
                    ORDER BY dead_lettered_at DESC, id DESC
                    """,
                    (queue_name,),
                ).fetchall()
            return [self._dead_letter_from_row(row) for row in rows]

    @_wrap_storage_errors
    def purge_terminal_messages(
        self,
        queue_name: str,
        older_than: datetime,
        *,
        include_dead_lettered: bool = False,
    ) -> int:
        queue_name = validate_queue_name(queue_name)
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                removed = 0
                rows = conn.execute(
                    """
                    SELECT id FROM messages
                    WHERE queue_name = ?
                      AND status IN (?, ?)
                      AND updated_at <= ?
                    """,
                    (
                        queue_name,
                        MessageStatus.ACKED.value,
                        MessageStatus.DELETED.value,
                        _dt(older_than),
                    ),
                ).fetchall()
                for row in rows:
                    if self._purge_message_locked(conn, int(row["id"])):
                        removed += 1
                if include_dead_lettered:
                    dlq_rows = conn.execute(
                        """
                        SELECT id FROM messages
                        WHERE queue_name = ?
                          AND status = ?
                          AND dead_lettered_at IS NOT NULL
                          AND dead_lettered_at <= ?
                        """,
                        (
                            queue_name,
                            MessageStatus.DEAD_LETTERED.value,
                            _dt(older_than),
                        ),
                    ).fetchall()
                    for row in dlq_rows:
                        message_id = int(row["id"])
                        conn.execute(
                            "DELETE FROM dead_letters WHERE original_message_id = ?",
                            (message_id,),
                        )
                        if self._purge_message_locked(conn, message_id):
                            removed += 1
                conn.commit()
                return removed
            except BaseException:
                conn.rollback()
                raise

    def _purge_message_locked(self, conn: sqlite3.Connection, message_id: int) -> bool:
        conn.execute(
            "DELETE FROM queue_events WHERE message_id = ?",
            (message_id,),
        )
        cur = conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
        return cur.rowcount == 1

    def _release_expired_leases_locked(
        self,
        conn: sqlite3.Connection,
        now: datetime,
        queue_name: str | None = None,
    ) -> LeaseReleaseResult:
        params: list[Any] = [MessageStatus.LEASED.value, _dt(now)]
        queue_clause = ""
        if queue_name is not None:
            queue_clause = " AND queue_name = ?"
            params.append(queue_name)
        rows = conn.execute(
            f"""
            SELECT * FROM messages
            WHERE status = ?
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= ?{queue_clause}
            ORDER BY lease_expires_at ASC, id ASC
            """,
            params,
        ).fetchall()
        redelivered = 0
        dead_lettered = 0
        for row in rows:
            if decide_retry(int(row["attempts"]), int(row["max_attempts"])).should_dead_letter:
                if self._move_to_dlq_locked(conn, row, DLQ_REASON_LEASE_EXPIRED, now):
                    dead_lettered += 1
                continue
            assert_legal_transition(MessageStatus.LEASED, MessageStatus.AVAILABLE)
            cur = conn.execute(
                """
                UPDATE messages
                SET status = ?, available_at = ?, updated_at = ?,
                    leased_at = NULL, lease_expires_at = NULL,
                    receipt_handle = NULL, worker_id = NULL,
                    last_error = ?, redeliveries = redeliveries + 1,
                    version = version + 1
                WHERE id = ? AND status = ?
                """,
                (
                    MessageStatus.AVAILABLE.value,
                    _dt(now),
                    _dt(now),
                    "lease expired",
                    int(row["id"]),
                    MessageStatus.LEASED.value,
                ),
            )
            if cur.rowcount == 1:
                self._record_event(
                    conn,
                    events.LEASE_EXPIRED,
                    str(row["queue_name"]),
                    int(row["id"]),
                    row["worker_id"],
                    row["receipt_handle"],
                    {"attempt": int(row["attempts"])},
                    now,
                )
                redelivered += 1
        return LeaseReleaseResult(redelivered=redelivered, dead_lettered=dead_lettered)

    def _nack_expired_lease_locked(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        now: datetime,
        reason: str | None,
    ) -> NackResult:
        message_id = int(row["id"])
        if decide_retry(int(row["attempts"]), int(row["max_attempts"])).should_dead_letter:
            if not self._move_to_dlq_locked(
                conn,
                row,
                reason or DLQ_REASON_LEASE_EXPIRED,
                now,
            ):
                conn.commit()
                return NackResult(False, message_id, MessageStatus.LEASED.value, "stale_receipt")
            self._record_event(
                conn,
                events.NACK,
                str(row["queue_name"]),
                message_id,
                row["worker_id"],
                row["receipt_handle"],
                {"attempt": int(row["attempts"]), "reason": reason, "moved_to_dlq": True},
                now,
            )
            conn.commit()
            return NackResult(
                True,
                message_id,
                MessageStatus.DEAD_LETTERED.value,
                reason,
                moved_to_dlq=True,
            )

        assert_legal_transition(MessageStatus.LEASED, MessageStatus.AVAILABLE)
        cur = conn.execute(
            """
            UPDATE messages
            SET status = ?, available_at = ?, updated_at = ?,
                leased_at = NULL, lease_expires_at = NULL,
                receipt_handle = NULL, worker_id = NULL,
                last_error = ?, redeliveries = redeliveries + 1,
                version = version + 1
            WHERE id = ? AND status = ?
            """,
            (
                MessageStatus.AVAILABLE.value,
                _dt(now),
                _dt(now),
                reason or "lease expired",
                message_id,
                MessageStatus.LEASED.value,
            ),
        )
        if cur.rowcount != 1:
            conn.commit()
            return NackResult(False, message_id, MessageStatus.LEASED.value, "stale_receipt")
        self._record_event(
            conn,
            events.NACK,
            str(row["queue_name"]),
            message_id,
            row["worker_id"],
            row["receipt_handle"],
            {"attempt": int(row["attempts"]), "reason": reason, "lease_expired": True},
            now,
        )
        conn.commit()
        return NackResult(True, message_id, MessageStatus.AVAILABLE.value, reason)

    def _move_to_dlq_locked(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        reason: str | None,
        now: datetime,
    ) -> bool:
        message_id = int(row["id"])
        assert_legal_transition(MessageStatus(str(row["status"])), MessageStatus.DEAD_LETTERED)
        cur = conn.execute(
            """
            UPDATE messages
            SET status = ?, dead_lettered_at = ?, updated_at = ?,
                leased_at = NULL, lease_expires_at = NULL,
                receipt_handle = NULL, worker_id = NULL,
                last_error = ?, version = version + 1
            WHERE id = ? AND status IN (?, ?)
            """,
            (
                MessageStatus.DEAD_LETTERED.value,
                _dt(now),
                _dt(now),
                reason,
                message_id,
                MessageStatus.LEASED.value,
                MessageStatus.AVAILABLE.value,
            ),
        )
        if cur.rowcount != 1:
            return False
        conn.execute(
            """
            INSERT INTO dead_letters (
                original_message_id, queue_name, payload, failure_reason,
                attempts, created_at, dead_lettered_at, final_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                row["queue_name"],
                row["payload"],
                reason,
                int(row["attempts"]),
                row["created_at"],
                _dt(now),
                MessageStatus.DEAD_LETTERED.value,
            ),
        )
        self._record_event(
            conn,
            events.DEAD_LETTER,
            str(row["queue_name"]),
            message_id,
            row["worker_id"],
            row["receipt_handle"],
            {"reason": reason, "attempts": int(row["attempts"])},
            now,
        )
        return True

    def _record_event(
        self,
        conn: sqlite3.Connection,
        event_type: str,
        queue_name: str,
        message_id: int | None,
        worker_id: str | None,
        receipt_handle: str | None,
        details: dict[str, Any],
        now: datetime,
    ) -> None:
        conn.execute(
            """
            INSERT INTO queue_events (
                event_type, queue_name, message_id, worker_id,
                receipt_handle_short, details, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_type,
                queue_name,
                message_id,
                worker_id,
                _short(receipt_handle),
                json.dumps(details, default=str, sort_keys=True),
                _dt(now),
            ),
        )

    def _message_from_row(self, row: sqlite3.Row) -> Message:
        return Message(
            id=int(row["id"]),
            queue_name=str(row["queue_name"]),
            payload=loads(str(row["payload"])),
            status=MessageStatus(str(row["status"])),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            created_at=self._required_dt(row["created_at"]),
            updated_at=self._required_dt(row["updated_at"]),
            available_at=self._required_dt(row["available_at"]),
            leased_at=_parse_dt(row["leased_at"]),
            lease_expires_at=_parse_dt(row["lease_expires_at"]),
            acked_at=_parse_dt(row["acked_at"]),
            dead_lettered_at=_parse_dt(row["dead_lettered_at"]),
            idempotency_key=row["idempotency_key"],
            last_error=row["last_error"],
            version=int(row["version"]),
            receipt_handle=row["receipt_handle"],
            worker_id=row["worker_id"],
            redeliveries=int(row["redeliveries"]),
        )

    def _dead_letter_from_row(self, row: sqlite3.Row) -> DeadLetter:
        return DeadLetter(
            id=int(row["id"]),
            original_message_id=int(row["original_message_id"]),
            queue_name=str(row["queue_name"]),
            payload=loads(str(row["payload"])),
            failure_reason=row["failure_reason"],
            attempts=int(row["attempts"]),
            created_at=self._required_dt(row["created_at"]),
            dead_lettered_at=self._required_dt(row["dead_lettered_at"]),
            final_status=MessageStatus(str(row["final_status"])),
            requeued_at=_parse_dt(row["requeued_at"]),
            requeued_message_id=(
                int(row["requeued_message_id"]) if row["requeued_message_id"] is not None else None
            ),
        )

    def _event_from_row(self, row: sqlite3.Row) -> QueueEvent:
        return QueueEvent(
            id=int(row["id"]),
            event_type=str(row["event_type"]),
            queue_name=str(row["queue_name"]),
            message_id=int(row["message_id"]) if row["message_id"] is not None else None,
            worker_id=row["worker_id"],
            receipt_handle_short=row["receipt_handle_short"],
            details=json.loads(str(row["details"])),
            created_at=self._required_dt(row["created_at"]),
        )

    @staticmethod
    def _required_dt(value: str | None) -> datetime:
        parsed = _parse_dt(value)
        if parsed is None:
            raise ValueError("expected a non-null timestamp")
        return parsed
