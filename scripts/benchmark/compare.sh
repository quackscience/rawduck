#!/usr/bin/env zsh
# Compare a benchmark JSON result against a saved baseline (within tolerance).
#
# usage: ./scripts/benchmark/compare.sh RESULT.json [BASELINE.json] [--min-ratio 0.90]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULT="${1:?result json required}"
BASELINE="${2:-${ROOT}/benchmark/results/baseline-otel.json}"
MIN_RATIO=0.98

if [[ "${2:-}" == "--min-ratio" ]]; then
	MIN_RATIO=$3
	BASELINE="${ROOT}/benchmark/results/baseline-otel.json"
elif [[ "${3:-}" == "--min-ratio" ]]; then
	MIN_RATIO=$4
fi

if [[ ! -f "${RESULT}" ]]; then
	echo "missing result: ${RESULT}" >&2
	exit 1
fi
if [[ ! -f "${BASELINE}" ]]; then
	echo "missing baseline: ${BASELINE} (run ./scripts/benchmark/run_otel.sh --quick first)" >&2
	exit 1
fi

python3 - "${RESULT}" "${BASELINE}" "${MIN_RATIO}" <<'PY'
import json
import sys

result_path, baseline_path, min_ratio = sys.argv[1:4]
min_ratio = float(min_ratio)

with open(result_path) as f:
    result = json.load(f)
with open(baseline_path) as f:
    baseline = json.load(f)

failures = []
for key, base in baseline.get("results", {}).items():
    cur = result.get("results", {}).get(key)
    if not cur:
        failures.append(f"missing metric: {key}")
        continue
    base_rps = base.get("records_per_sec", 0)
    cur_rps = cur.get("records_per_sec", 0)
    if base_rps <= 0:
        continue
    ratio = cur_rps / base_rps
    print(f"{key}: {cur_rps} rec/s vs baseline {base_rps} ({ratio:.1%})")
    if ratio < min_ratio:
        failures.append(f"{key}: {ratio:.1%} < {min_ratio:.0%} of baseline")

if failures:
    print("REGRESSION:", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    raise SystemExit(1)
print("OK: within tolerance")
PY
