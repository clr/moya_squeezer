#!/bin/zsh

set -euo pipefail

NETWORK_NAME="${NETWORK_NAME:-moya_net}"
IMAGE_NAME="${IMAGE_NAME:-moya_squeezer:latest}"
DB_IMAGE="${DB_IMAGE:-moya_db:latest}"
CONFIG_PATH="${CONFIG_PATH:-config/docker.toml}"
COOKIE="${COOKIE:-squeeze_cookie}"
BASE_IMAGE="${BASE_IMAGE:-elixir:1.19.0}"

echo "[cluster] building squeezer image: ${IMAGE_NAME}"
docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t "${IMAGE_NAME}" /Users/clr/moya_squeezer

echo "[cluster] ensuring network: ${NETWORK_NAME}"
docker network create "${NETWORK_NAME}" >/dev/null 2>&1 || true

echo "[cluster] clearing old containers (if any)"
docker rm -f moya_db manager worker1 worker2 worker3 >/dev/null 2>&1 || true

echo "[cluster] starting db"
docker run -d \
  --name moya_db \
  --network "${NETWORK_NAME}" \
  -p 9000:9000 \
  "${DB_IMAGE}" >/dev/null

echo "[cluster] starting workers"
for WORKER in worker1 worker2 worker3; do
  docker run -d \
    --name "${WORKER}" \
    --hostname "${WORKER}" \
    --network "${NETWORK_NAME}" \
    "${IMAGE_NAME}" \
    sh -lc "while true; do ERL_LIBS=/app/_build/dev/lib elixir --sname ${WORKER} --cookie ${COOKIE} -e 'Application.ensure_all_started(:moya_squeezer); case MoyaSqueezer.run_worker(:\"manager@manager\") do :ok -> :ok; {:error, reason} -> IO.puts(reason); System.halt(1) end'; echo '[worker retry] waiting for manager'; sleep 2; done" >/dev/null
done

echo "[cluster] starting manager"
docker run -d \
  --name manager \
  --hostname manager \
  --network "${NETWORK_NAME}" \
  -v /Users/clr/moya_squeezer/config:/app/config:ro \
  -v /Users/clr/moya_squeezer/logs:/app/logs \
  "${IMAGE_NAME}" \
  sh -lc "while true; do ERL_LIBS=/app/_build/dev/lib elixir --sname manager --cookie ${COOKIE} -e 'Application.ensure_all_started(:moya_squeezer); case MoyaSqueezer.run(\"${CONFIG_PATH}\", worker_nodes: [:\"worker1@worker1\", :\"worker2@worker2\", :\"worker3@worker3\"]) do :ok -> :ok; {:error, reason} -> IO.puts(reason); System.halt(1) end' && break; echo '[manager retry] waiting for workers'; sleep 2; done"

echo "[cluster] started. tail manager logs with: docker logs -f manager"
echo "[cluster] stop with: /Users/clr/moya_squeezer/scripts/stop_cluster.sh"
