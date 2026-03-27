#!/bin/zsh

set -euo pipefail

ROLE="${ROLE:-manager}"
CONFIG_PATH="${1:-config/local.toml}"

if ! mix deps.loadpaths >/dev/null 2>&1; then
  echo "Dependencies missing or not compiled. Running mix deps.get..."
  mix deps.get
fi

if [[ "$ROLE" == "manager" ]]; then
  if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "Config file not found: $CONFIG_PATH" >&2
    echo "Usage: ROLE=manager ./scripts/squeezer.sh [path/to/config.toml]" >&2
    exit 1
  fi

  mix squeezer.run "$CONFIG_PATH" "${@:2}"
elif [[ "$ROLE" == "worker" ]]; then
  if [[ -z "${MANAGER_NODE:-}" ]]; then
    echo "MANAGER_NODE is required when ROLE=worker" >&2
    echo "Usage: ROLE=worker MANAGER_NODE=manager@host ./scripts/squeezer.sh" >&2
    exit 1
  fi

  mix squeezer.run --role worker --manager "$MANAGER_NODE"
else
  echo "Invalid ROLE='$ROLE'. Use manager or worker." >&2
  exit 1
fi