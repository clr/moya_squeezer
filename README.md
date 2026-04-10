# moya_squeezer

Squeeze-testing framework for driving load against an existing database API.

Canonical repository:

- https://github.com/clr/moya_squeezer

Related repositories:

- Deployment/orchestration: https://github.com/clr/moya_harness
- Database service (Moya): https://github.com/clr/moya

## What it does

- Reads a TOML file that defines load dimensions.
- Starts one OTP process per configured logical connection.
- Generates `read`/`write`/`delete` traffic at the configured requests/second rate.
- Writes per-request metrics to a log file, buffered and flushed at a configurable interval (`metrics_flush_interval_ms`, default 10ms).
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

### Select HTTP adapter

You can choose request adapter at runtime:

- Finch (default): `--adapter finch`
- OTP inets/httpc: `--adapter httpc`

Examples:

```bash
mix squeezer.run config/local.toml --role manager --adapter finch
mix squeezer.run config/local.toml --role manager --adapter httpc
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

During a run, workers keep request-path state local (local key tracking + local metrics logging).
The primary steady-state backhaul to manager is periodic stats batches; manager also issues control-plane
RPCs for ramp/mode updates.

### Script mode

- Manager:

```bash
ROLE=manager ./scripts/squeezer.sh config/local.toml --worker worker1@your-host
```

- Worker:

```bash
ROLE=worker MANAGER_NODE=manager@your-host ./scripts/squeezer.sh
```

## Run in containers (moved to `moya_harness`)

Multi-service deployment/orchestration for `moya_squeezer` + `moya_db` now lives in the sibling repo:

- `/Users/clr/moya_harness`

Use that repo for:

- docker-compose stack runs
- cluster startup/teardown scripts
- deployment docs and CI/CD harnessing

`moya_squeezer` remains focused on squeeze app logic and config.

Container runtime config used by manager remains in this repo at:

- `config/docker.toml`

with DB target:

`base_url = "http://moya_db:9000"`

## Benchmarking `metrics_flush_interval_ms`

Use the helper script to run the same workload across multiple flush intervals and
produce a summary CSV:

```bash
zsh scripts/benchmark_flush_interval.sh config/local.toml
```

Optional environment variables:

- `INTERVALS` (default `5,10,20,50`)
- `OUT_DIR` (default `logs`)
- `RUN_CMD` (default `./scripts/squeezer.sh`)

Example:

```bash
INTERVALS=5,10,20,50 OUT_DIR=logs zsh scripts/benchmark_flush_interval.sh config/local.toml
```

The script writes one run log per interval and a summary CSV with:

`interval_ms,stop_reason,total,error_rate_pct,p95_ms,run_log`

## Benchmarking Finch vs httpc side-by-side

Use:

```bash
zsh scripts/benchmark_adapters.sh config/local.toml
```

Optional env vars:

- `ADAPTERS` (default `finch,httpc`)
- `OUT_DIR` (default `logs`)
- `RUN_CMD` (default `./scripts/squeezer.sh`)

## Config fields (TOML)

- `connections_per_worker`: Number of concurrent connection workers per worker node.
- `requests_per_second`: Backward-compatible default for starting throughput.
- `start_requests_per_second`: Initial target throughput for squeeze ramp (defaults to `requests_per_second`).
- `rps_step`: Amount to increase total target RPS at each ramp step (default `0`).
- `step_interval_seconds`: Seconds between ramp steps (default `5`).
- `ramp_mode`: Ramp strategy, `rps` or `concurrency` (default `rps`).
- `total_target_rps`: Total measured-phase target RPS used by `concurrency` mode (default `requests_per_second`).
- `initial_active_workers`: Starting active worker count in `concurrency` mode (default `1`).
- `worker_step`: Active workers to add per ramp step in `concurrency` mode (default `1`).
- `worker_step_interval_seconds`: Seconds between worker activation steps in `concurrency` mode (default `step_interval_seconds`).
- `worker_container_pool`: Required total worker containers for `concurrency` mode. Manager requires discovered workers to equal this value before run start (defaults to legacy `max_active_workers` if present, else `connections_per_worker`).
- `baseline_window_seconds`: Baseline measurement window after burn-in, used to compute baseline p90 (default `10`).
- `max_error_rate_pct`: Error-rate stop threshold percentage for measured phase (default `1.0`).
- `error_breach_consecutive_windows`: Number of consecutive measured windows with error rate above `max_error_rate_pct` required before stopping (default `1`).
- `stop_latency_percentile`: Baseline percentile used for latency stop threshold (default `0.90`, i.e. p90).
- `latency_breach_consecutive_windows`: Number of consecutive measured windows with `p50 > baseline_percentile` required before stopping (default `1`).
- `worker_tick_ms`: Worker token-bucket scheduler interval in ms (default `10`).
- `worker_inflight_limit`: Max concurrent in-flight requests per worker process (default `1`).
- `read_ratio`, `write_ratio`, `delete_ratio`: Must sum to `1.0`.
- `payload_size`: Payload bytes used by write calls.
- `duration_seconds`: How long to run the test.
- `warmup_seconds`: Optional write-only burn-in duration before measured traffic starts (default `0`).
- `request_timeout_ms`: Adapter request timeout in ms (default `5000`).
- `max_retries`: Retry attempts for transport errors and HTTP 5xx (default `0`).
- `retry_backoff_ms`: Linear retry backoff base in ms (default `25`).
- `metrics_flush_interval_ms`: Buffered metrics flush interval in ms (default `10`).
- `metrics_compact`: Enables compact aggregated metrics CSV output (default `true`).
- `stats_flush_interval_ms`: Worker-local stats flush interval to manager in ms (default `100`).
- `base_url`: API base URL (defaults to `http://localhost:9000`).
- `read_path`, `write_path`, `delete_path`: Endpoint base paths (defaults `/db/v0.1` for moya_db compatibility).
- `log_path`: Append-only metrics log path.

## Metrics log format

CSV columns:

- Compact mode (`metrics_compact = true`, default):
  `bucket_ms,source_node,request_type,response_code,count,sum_db_latency_us`
- Raw mode (`metrics_compact = false`):
  `bucket_ms,source_node,request_type,started_at_ms,db_latency_us,response_code`

- `bucket_ms` is rounded down by `metrics_flush_interval_ms` from `started_at_ms`.
- `response_code` is `0` when the request errors before receiving an HTTP response.

## Runtime console output

Per-second line:

`[sec] rps=... errors=... error_rate=...% p50=...ms p90=...ms p95=...ms`

Final line:

`[final] stop_reason=... total=... errors=... error_rate=...% avg=...ms p50=...ms p90=...ms p95=...ms`

Per-worker measured summary table:

`[manager][workers] id\tnode\trequests\tavg_rps`

## Burn-in behavior

- During warmup (`warmup_seconds > 0`), workers send write-only traffic to seed known keys.
- After warmup, measured traffic begins and stats are reset for the measured phase.
- The runner now includes an internal key pool scaffold so reads/deletes preferentially target known keys.

## Squeeze control loop

After burn-in, the runner:

1. Runs baseline collection for `baseline_window_seconds`.
2. Captures baseline p90 latency.
3. Ramps load according to `ramp_mode`:
   - `rps`: starts/increments target RPS from `start_requests_per_second` by `rps_step` every `step_interval_seconds`.
  - `concurrency`: starts with `initial_active_workers` and activates additional workers by `worker_step` every `worker_step_interval_seconds` up to `worker_container_pool`, redistributing `total_target_rps` across active workers.
4. Stops when one of the following occurs:
   - `duration_seconds` elapsed
   - measured error rate exceeds `max_error_rate_pct`
   - measured p50 latency exceeds baseline `stop_latency_percentile` latency for `latency_breach_consecutive_windows` consecutive windows
