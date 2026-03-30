#!/bin/zsh

set -euo pipefail

BASE_CONFIG="${1:-config/local.toml}"
INTERVALS_CSV="${INTERVALS:-0.05,0.5,1,5}"
OUT_DIR="${OUT_DIR:-logs}"
RUN_CMD="${RUN_CMD:-./scripts/squeezer.sh}"

if [[ ! -f "$BASE_CONFIG" ]]; then
  echo "Base config not found: $BASE_CONFIG" >&2
  echo "Usage: ./scripts/benchmark_flush_interval.sh [path/to/base_config.toml]" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

timestamp="$(date +%Y%m%d_%H%M%S)"
summary_csv="$OUT_DIR/flush_interval_benchmark_${timestamp}.csv"

echo "interval_ms,stop_reason,total,error_rate_pct,p95_ms,run_log" > "$summary_csv"

echo "Running flush-interval benchmark"
echo "  base config: $BASE_CONFIG"
echo "  intervals:   $INTERVALS_CSV"
echo "  summary:     $summary_csv"

IFS=',' read -r -A intervals <<< "$INTERVALS_CSV"

for interval in "${intervals[@]}"; do
  if ! [[ "$interval" =~ '^[0-9]+$' ]] || [[ "$interval" -le 0 ]]; then
    echo "Skipping invalid interval: $interval" >&2
    continue
  fi

  tmp_config="$OUT_DIR/benchmark_${interval}ms_${timestamp}.toml"
  run_log="$OUT_DIR/benchmark_${interval}ms_${timestamp}.log"

  cp "$BASE_CONFIG" "$tmp_config"

  if grep -q '^metrics_flush_interval_ms\s*=' "$tmp_config"; then
    sed -i '' -E "s/^metrics_flush_interval_ms\s*=.*/metrics_flush_interval_ms = ${interval}/" "$tmp_config"
  else
    printf "\n# Metrics logger controls\nmetrics_flush_interval_ms = %s\n" "$interval" >> "$tmp_config"
  fi

  echo "\n==> interval=${interval}ms"
  if ROLE=manager "$RUN_CMD" "$tmp_config" > "$run_log" 2>&1; then
    final_line="$(grep '\[manager\]\[final\]' "$run_log" | tail -n 1 || true)"

    if [[ -z "$final_line" ]]; then
      echo "No final line found for interval ${interval}. See $run_log" >&2
      echo "${interval},unknown,0,0,0,${run_log}" >> "$summary_csv"
      continue
    fi

    stop_reason="$(echo "$final_line" | sed -nE 's/.*stop_reason=([^ ]+).*/\1/p')"
    total="$(echo "$final_line" | sed -nE 's/.*total=([0-9]+).*/\1/p')"
    error_rate="$(echo "$final_line" | sed -nE 's/.*error_rate=([0-9.]+)%.*/\1/p')"
    p95="$(echo "$final_line" | sed -nE 's/.*p95=([0-9.]+)ms.*/\1/p')"

    echo "${interval},${stop_reason},${total},${error_rate},${p95},${run_log}" >> "$summary_csv"
    echo "    stop_reason=${stop_reason} total=${total} error_rate=${error_rate}% p95=${p95}ms"
  else
    echo "Run failed for interval ${interval}. See $run_log" >&2
    echo "${interval},failed,0,0,0,${run_log}" >> "$summary_csv"
  fi
done

echo "\nBenchmark complete."
echo "Summary CSV: $summary_csv"
echo ""
column -s, -t "$summary_csv" || cat "$summary_csv"
