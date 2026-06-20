""" Produce command. """

from __future__ import annotations

import argparse
import json
import sys
from json import JSONDecodeError
from typing import Any

from simplequeue.cli._shared import make_queue, print_json
from simplequeue.config import QueueConfig
from simplequeue.reliability.idempotency import payload_idempotency_key


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("produce", parents=parents, help="enqueue one or more messages")
    parser.add_argument("--queue", help="queue name")
    parser.add_argument("--payload", help="JSON payload (default: {\"n\": message number})")
    parser.add_argument("--payload-template", help="JSON template using {n}")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--idempotency-key", help="dedupe key (with --count > 1, suffixes :1, :2, …)")
    parser.add_argument(
        "--idempotent",
        action="store_true",
        help="derive an idempotency key from each payload (dedupes identical payloads)",
    )
    parser.add_argument("--max-attempts", type=int)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    if args.payload and args.payload_template:
        raise ValueError("use only one of --payload or --payload-template")
    queue = make_queue(args, config)
    queue.init_schema()
    if args.count < 1:
        raise ValueError("--count must be >= 1")
    if args.idempotent and args.count > 1 and not args.payload_template:
        print(
            "simplequeue: warning: --idempotent with --count > 1 and identical payloads "
            "dedupes to one message; use --payload-template or explicit --idempotency-key suffixes",
            file=sys.stderr,
        )
    message_ids: list[int] = []
    for number in range(1, args.count + 1):
        payload = _payload_for(args, number)
        key = args.idempotency_key
        if key and args.count > 1:
            key = f"{key}:{number}"
        elif key is None and args.idempotent:
            key = payload_idempotency_key(payload)
        message_ids.append(queue.enqueue(payload, idempotency_key=key, max_attempts=args.max_attempts))
    print_json(
        {
            "queue": queue.queue_name,
            "message_ids": message_ids,
            "count": len(set(message_ids)),  # distinct rows actually enqueued (dedup-aware)
            "requested": args.count,
        }
    )
    return 0


def _payload_for(args: argparse.Namespace, number: int) -> Any:
    try:
        if args.payload_template:
            return json.loads(args.payload_template.replace("{n}", str(number)))
        if args.payload:
            return json.loads(args.payload)
    except JSONDecodeError as error:
        raise ValueError(f"invalid JSON payload: {error}") from error
    return {"n": number}
