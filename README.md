# moya_squeezer

Squeeze-testing framework for driving load against an existing database API.

## What it does

- Reads a TOML file that defines load dimensions.
- Starts one OTP process per configured logical connection.
- Generates `read`/`write`/`delete` traffic at the configured requests/second rate.
- Writes per-request metrics to a log file, buffered and flushed every 5ms.
- Prints per-second runtime stats (achieved RPS, error rate, p50/p90/p95 latency).
- Handles Ctrl+C gracefully and prints a final summary report.

## Run

1. Install dependencies:

```bash
mix deps.get
```

2. Copy and edit the example config:

```bash
cp config/example.toml config/local.toml
```

3. Start load test as a **manager** (script wrapper, no `mix` command needed):

```bash
ROLE=manager ./scripts/squeezer.sh config/local.toml
```

The script auto-runs `mix deps.get` only when dependencies are missing.

Or run the Mix task directly as manager:

```bash
mix squeezer.run config/local.toml --role manager
```

## Distributed manager/worker mode

This app can run in two roles:

- **Manager**: coordinates the squeeze test and aggregates stats.
- **Worker**: joins the manager node and runs worker segments via RPC.

Use standard distributed Elixir node flags (`--sname`/`--name`, `--cookie`) for clustering.

### 1) Start worker node(s)

```bash
iex --sname worker1 --cookie squeeze -S mix squeezer.run --role worker --manager manager@your-host
```

### 2) Start manager node

```bash
iex --sname manager --cookie squeeze -S mix squeezer.run config/local.toml --role manager --worker worker1@your-host
```

You can repeat `--worker` multiple times to add more worker nodes.

Worker nodes connect using `Node.connect/1`/`Node.ping/1`; manager orchestration is performed via `:rpc.call/4`.

### Script mode

- Manager:

```bash
ROLE=manager ./scripts/squeezer.sh config/local.toml --worker worker1@your-host
```

- Worker:

```bash
ROLE=worker MANAGER_NODE=manager@your-host ./scripts/squeezer.sh
```

## Run in containers (Docker Compose)

This repo includes container artifacts for running both `moya_db` and `moya_squeezer` together.

The compose setup codifies a **1 Manager + 3 Workers** topology:

- `manager` runs the squeeze coordinator node (`manager@manager`)
- `worker1`, `worker2`, `worker3` run worker nodes
- all nodes share the same Erlang cookie (`squeeze_cookie`)
- workers join via `--manager manager@manager`

1. Ensure `moya_db` source exists at `~/moya_db` (used as compose build context).
2. Build images:

```bash
docker compose build
```

3. Run manager + workers + db:

```bash
docker compose up --abort-on-container-exit
```

This launches one `moya_squeezer:latest` image as 4 containers (1 manager, 3 workers).

4. Tear down:

```bash
docker compose down
```

### If Compose is unavailable on your machine

Use the provided scripts to run the same 1-manager + 3-worker topology with plain Docker:

```bash
chmod +x scripts/run_cluster.sh scripts/stop_cluster.sh
./scripts/run_cluster.sh
```

Stop and clean up:

```bash
./scripts/stop_cluster.sh
```

Notes:

- `scripts/run_cluster.sh` builds `moya_squeezer:latest`, creates `moya_net`, starts `moya_db`, then starts `worker1..3` and `manager`.
- Workers join manager using distributed node names and shared cookie (`squeeze_cookie`).
- Override defaults with env vars: `IMAGE_NAME`, `DB_IMAGE`, `NETWORK_NAME`, `COOKIE`, `CONFIG_PATH`.
- For Colima/no-Docker-Hub workflows, the Dockerfile supports a local base image via `BASE_IMAGE` (default `elixir:1.19.0`). Example: `BASE_IMAGE=elixir:1.19.0 ./scripts/run_cluster.sh`.

Container config lives at `config/docker.toml` and targets:

`base_url = "http://moya_db:9000"`

## Config fields (TOML)

- `connections`: Number of concurrent connection workers.
- `requests_per_second`: Backward-compatible default for starting throughput.
- `start_requests_per_second`: Initial target throughput for squeeze ramp (defaults to `requests_per_second`).
- `rps_step`: Amount to increase total target RPS at each ramp step (default `0`).
- `step_interval_seconds`: Seconds between ramp steps (default `5`).
- `baseline_window_seconds`: Baseline measurement window after burn-in, used to compute baseline p90 (default `10`).
- `max_error_rate_pct`: Error-rate stop threshold percentage for measured phase (default `1.0`).
- `read_ratio`, `write_ratio`, `delete_ratio`: Must sum to `1.0`.
- `payload_size`: Payload bytes used by write calls.
- `duration_seconds`: How long to run the test.
- `warmup_seconds`: Optional write-only burn-in duration before measured traffic starts (default `0`).
- `request_timeout_ms`: Adapter request timeout in ms (default `5000`).
- `max_retries`: Retry attempts for transport errors and HTTP 5xx (default `0`).
- `retry_backoff_ms`: Linear retry backoff base in ms (default `25`).
- `base_url`: API base URL (defaults to `http://localhost:9000`).
- `read_path`, `write_path`, `delete_path`: Endpoint base paths (defaults `/db/v0.1` for moya_db compatibility).
- `log_path`: Append-only metrics log path.

## Metrics log format

CSV columns:

`bucket_ms,request_type,started_at_ms,db_latency_us,response_code`

- `bucket_ms` is rounded down to 5ms buckets from `started_at_ms`.
- `response_code` is `0` when the request errors before receiving an HTTP response.

## Runtime console output

Per-second line:

`[sec] rps=... errors=... error_rate=...% p50=...ms p90=...ms p95=...ms`

Final line:

`[final] stop_reason=... total=... errors=... error_rate=...% avg=...ms p50=...ms p90=...ms p95=...ms`

## Burn-in behavior

- During warmup (`warmup_seconds > 0`), workers send write-only traffic to seed known keys.
- After warmup, measured traffic begins and stats are reset for the measured phase.
- The runner now includes an internal key pool scaffold so reads/deletes preferentially target known keys.

## Squeeze control loop

After burn-in, the runner:

1. Runs baseline collection for `baseline_window_seconds`.
2. Captures baseline p90 latency.
3. Starts/increments target RPS from `start_requests_per_second` by `rps_step` every `step_interval_seconds`.
4. Stops when one of the following occurs:
   - `duration_seconds` elapsed
   - measured error rate exceeds `max_error_rate_pct`
   - measured p50 latency exceeds baseline p90 latency
