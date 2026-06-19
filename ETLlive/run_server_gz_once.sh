#!/usr/bin/env bash
set -euo pipefail

LIVE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(dirname "$LIVE_ROOT")"
PYTHON="${PYTHON:-$WORKSPACE_ROOT/venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="${PYTHON:-python3}"
fi

CONFIG="${VETO_LIVE_SERVER_CONFIG:-$LIVE_ROOT/config/server_live_config.json}"
SOURCE="${1:-all}"

exec "$PYTHON" "$LIVE_ROOT/src/server_gz_worker.py" \
  --once \
  --config "$CONFIG" \
  --source "$SOURCE"
