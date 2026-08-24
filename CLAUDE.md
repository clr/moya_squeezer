# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A squeeze-testing (load-testing) framework for driving traffic against the `moya` database API. It reads
a TOML config describing load dimensions, spins up OTP processes per logical connection, ramps traffic
according to a configurable strategy, and stops automatically on error-rate or latency breaches.

Deployment/orchestration (docker-compose, cluster scripts) lives in the sibling repo `moya_harness`
(`/Users/clr/moya_harness`), not here. This repo is app logic and config only.

## Commands

```bash
mix deps.get                        # install deps
mix test                            # run all tests
mix test test/moya_squeezer/runner_test.exs   # run a single test file
mix test path/to/test.exs:42        # run a single test by line
mix format                          # format code (see .formatter.exs)
mix mix_audit                       # dependency vulnerability audit (dev/test only)
```

Run a squeeze test as manager (script wrapper, auto-runs `deps.get` if needed):

```bash
ROLE=manager ./scripts/squeezer.sh config/local.toml
```

Or directly via the Mix task, with adapter selection (`finch` default, `httpc`, or `mint`):

```bash
mix squeezer.run config/local.toml --role manager --adapter finch
```

Distributed manager/worker mode uses standard distributed-Erlang flags (`--sname`/`--name`, `--cookie`);
see [README.md](README.md) for the full manager/worker startup sequence. Manager is control-plane only —
it refuses to run without at least one `--worker` node.

Benchmark helpers: `scripts/benchmark_flush_interval.sh` (sweeps `metrics_flush_interval_ms`) and
`scripts/benchmark_adapters.sh` (Finch vs httpc side-by-side).

## Architecture

**Process topology.** `MoyaSqueezer.Application` starts a global `Finch` pool, `MoyaSqueezer.RuntimeState`
(an `Agent` holding cross-cutting runtime info: role, dispatch stats, worker event window), and an optional
`Plug.Cowboy` server (`MoyaSqueezer.MetricsRouter`, default port 4001) exposing `/worker/v0.1/metrics` and
`/manager/v0.1/metrics` JSON endpoints backed by `MoyaSqueezer.MetricsApi`.

**Manager/worker split** (all in [runner.ex](lib/moya_squeezer/runner.ex), by far the largest module —
read it top-to-bottom before changing ramp/stop logic):
- The **manager** node parses config, connects to worker nodes (`Node.connect/1`), and issues control-plane
  RPCs (`:rpc.call/4`) to start/stop worker segments, change rate, switch mode, and pull stats/keyspace.
- Each **worker** node runs a `Supervisor` ("worker segment") of `ConnectionWorker` GenServers — one per
  logical connection (`connections_per_worker`). Workers keep request-path state fully local (local key
  tracking, local metrics logging) and only report back to the manager via periodic stats batches; this is
  what lets the load generation scale independently of manager-side aggregation.
- Three ramp strategies, selected by `ramp_mode` in config, each with its own manager loop in runner.ex:
  `:rps` (step up total target RPS), `:concurrency` (step up active worker count), `:payload` (step up
  payload size). `maybe_step_load/2` dispatches on `config.ramp_mode`.
- The squeeze control loop is: burn-in (optional write-only warmup to seed keys) → baseline window (capture
  baseline p90) → ramp per strategy → stop on `duration_seconds` elapsed, error-rate breach, or latency
  breach (measured p50 vs baseline percentile, over N consecutive windows). See README's "Squeeze control
  loop" section for the exact sequencing.

**Request path.** `ConnectionWorker` ([adapters/connection_worker.ex](lib/moya_squeezer/adapters/connection_worker.ex))
is a token-bucket scheduler: on each `tick_ms` it accrues `reqs_per_sec * tick_ms/1000` tokens and spawns
requests (bounded by `worker_inflight_limit`) via `Task.Supervisor`, each request going through the
configured adapter module. Adapters implement the `MoyaSqueezer.LoadAdapter` behaviour (`request/4` →
`{:ok, status, latency_us}` / `{:error, reason, latency_us}`) — see
[load_adapter.ex](lib/moya_squeezer/load_adapter.ex). Three adapters exist: `HttpAdapter` (Finch, default),
`HttpcAdapter` (OTP `:httpc`), `MintAdapter` (raw Mint connections, kept alive per-worker rather than
pooled). Retry/backoff logic is factored into the shared `MoyaSqueezer.Adapters.RequestRetrier`, which all
HTTP-based adapters delegate to for 5xx/error retry with linear backoff.

**Key tracking.** Each `ConnectionWorker` tracks its own written keys locally (`local_keys`/`local_key_list`)
so reads/deletes preferentially target known keys without cross-process coordination during the hot path.
Keyspace can be exported/imported across worker segments (`worker_segment_export_keyspace`/`import_keyspace`)
e.g. for handoff between ramp phases.

**Metrics.** `MoyaSqueezer.MetricsLogger` buffers per-request metrics and flushes to a CSV log file on
`metrics_flush_interval_ms`. Two log formats: compact (aggregated by bucket, default) or raw (one row per
request) — see README's "Metrics log format" for exact columns. `MoyaSqueezer.StatsCollector` aggregates
in-memory stats (RPS, error rate, latency percentiles) for the live console output and final summary;
distinct from `MetricsLogger`, which is the durable on-disk record.

**Config.** `MoyaSqueezer.Config` parses and validates the TOML file (via the vendored `toml` dep at
`vendor/toml`) into a struct, applying defaults for the many optional ramp/stop/timing fields. See README's
"Config fields" section for the full field list and defaults — don't duplicate that list when reading
config.ex, just check it against the struct.

## Testing notes

- `test/support/config_fixtures.ex` is compiled in `:test` env only (`elixirc_paths`) and provides shared
  config builders for tests.
- Tests that exercise the load path stub the adapter via `Application.put_env(:moya_squeezer, :load_adapter,
  FakeAdapter)` and restore it in `on_exit`, rather than hitting real HTTP.
- `test/tmp/` holds scratch CSV output written during metrics/adapter tests.
