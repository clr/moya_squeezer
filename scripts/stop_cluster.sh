#!/bin/zsh

set -euo pipefail

NETWORK_NAME="${NETWORK_NAME:-moya_net}"

echo "[cluster] stopping containers"
docker rm -f manager worker1 worker2 worker3 moya_db >/dev/null 2>&1 || true

echo "[cluster] removing network ${NETWORK_NAME}"
docker network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true

echo "[cluster] stopped"
