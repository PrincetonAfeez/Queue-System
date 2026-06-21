#!/usr/bin/env bash
# Example long-running consumer for simplequeue.
# Usage:
#   ./scripts/run-worker.sh
#   ./scripts/run-worker.sh --db /var/lib/app/queue.db --queue jobs --workers 2
set -euo pipefail

DB="queue.db"
QUEUE="default"
WORKERS="1"
CONFIG=""
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) DB="$2"; shift 2 ;;
    --queue) QUEUE="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --) shift; EXTRA=("$@"); break ;;
    *) EXTRA+=("$1"); shift ;;
  esac
done

ARGS=(consume --db "$DB" --queue "$QUEUE" --workers "$WORKERS" --mode at-least-once --sweeper)
if [[ -n "$CONFIG" ]]; then
  ARGS+=(--config "$CONFIG")
fi
ARGS+=("${EXTRA[@]}")

exec simplequeue "${ARGS[@]}"
