#!/bin/zsh

set -euo pipefail

BASE_CONFIG="${1:-config/local.toml}"
OUT_DIR="${OUT_DIR:-logs}"
RUN_CMD="${RUN_CMD:-./scripts/squeezer.sh}"
ADAPTERS_CSV="${ADAPTERS:-finch,httpc}"

if [[ ! -f "$BASE_CONFIG" ]]; then
  echo "Base config not found: $BASE_CONFIG" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
timestamp="$(date +%Y%m%d_%H%M%S)"
summary_csv="$OUT_DIR/adapter_benchmark_${timestamp}.csv"

echo "adapter,stop_reason,total,error_rate_pct,p95_ms,run_log" > "$summary_csv"

IFS=',' read -r -A adapters <<< "$ADAPTERS_CSV"

for adapter in "${adapters[@]}"; do
  run_log="$OUT_DIR/benchmark_${adapter}_${timestamp}.log"
  echo "\n==> adapter=${adapter}"

  if ROLE=manager "$RUN_CMD" "$BASE_CONFIG" --adapter "$adapter" > "$run_log" 2>&1; then
    final_line="$(grep '\[manager\]\[final\]' "$run_log" | tail -n 1 || true)"

    if [[ -z "$final_line" ]]; then
      echo "${adapter},unknown,0,0,0,${run_log}" >> "$summary_csv"
      continue
    fi

    stop_reason="$(echo "$final_line" | sed -nE 's/.*stop_reason=([^ ]+).*/\1/p')"
    total="$(echo "$final_line" | sed -nE 's/.*total=([0-9]+).*/\1/p')"
    error_rate="$(echo "$final_line" | sed -nE 's/.*error_rate=([0-9.]+)%.*/\1/p')"
    p95="$(echo "$final_line" | sed -nE 's/.*p95=([0-9.]+)ms.*/\1/p')"

    echo "${adapter},${stop_reason},${total},${error_rate},${p95},${run_log}" >> "$summary_csv"
    echo "    stop_reason=${stop_reason} total=${total} error_rate=${error_rate}% p95=${p95}ms"
  else
    echo "${adapter},failed,0,0,0,${run_log}" >> "$summary_csv"
  fi
done

echo "\nBenchmark complete."
echo "Summary CSV: $summary_csv"
column -s, -t "$summary_csv" || cat "$summary_csv"
