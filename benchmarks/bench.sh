#!/usr/bin/env bash
# Repeatable timing runs. Run from the repository root on the Windows machine.
#   bash benchmarks/bench.sh <env-file> <fast|legacy> [repetitions]
# Each repetition: reset the destinations, run Expand through the same service
# calls as the GUI, print MEASURED PROCESSING TOTAL, verify against the
# reference snapshot when one exists.
set -u
ENV_FILE="${1:?env file}"; ENGINE="${2:-fast}"; REPS="${3:-1}"
SERVER=${SERVER:-"wsl.exe -d Ubuntu -u root --"}
SERVER_REPO="${SERVER_REPO:-/mnt/d/kaggle/data-pipeline}"
PY="${PY:-.venv/Scripts/python.exe}"
export MSYS_NO_PATHCONV=1
mkdir -p .local
for rep in $(seq 1 "$REPS"); do
  $SERVER bash "$SERVER_REPO/benchmarks/reset_on_server.sh" "$SERVER_REPO/$ENV_FILE" > /dev/null 2>&1
  PDM_EXPAND_ENGINE=$ENGINE "$PY" tools/expand_cli.py --env "$ENV_FILE" run --quiet \
      --report ".local/report_${ENGINE}_${rep}.txt" 2>&1 | tail -1 > .local/bench_last.json
  total=$("$PY" -c "import json;d=json.load(open('.local/bench_last.json'));print(d['measured_processing_total_s'], 'rows', d['source_rows'])" 2> /dev/null || cat .local/bench_last.json)
  check=$($SERVER bash "$SERVER_REPO/benchmarks/server_sql.sh" "$SERVER_REPO/$ENV_FILE" kaggle_challenge -q -f "$SERVER_REPO/benchmarks/verify.sql" 2>&1 | tail -1)
  echo "engine=$ENGINE rep=$rep MEASURED_PROCESSING_TOTAL_s=$total verify=${check:-n/a}"
done
