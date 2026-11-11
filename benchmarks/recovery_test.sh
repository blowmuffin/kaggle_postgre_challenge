#!/usr/bin/env bash
# Failure-recovery scenarios for the Expand engine.
#
# Run from the repository root on the Windows machine (Git Bash), with the
# Linux server reachable as root through $SERVER (default: local WSL Ubuntu;
# e.g. SERVER="ssh root@server" for a remote box) and the repository visible
# there at $SERVER_REPO. Needs a reference snapshot (see benchmarks/README.md).
#
#   bash benchmarks/recovery_test.sh .local/bench.env manager_kill
#   scenarios: manager_kill | runner_kill | postgres_crash | abort | all
set -u
ENV_FILE="${1:?env file}"; SCENARIO="${2:-all}"; MIN_DONE="${MIN_DONE:-40}"
SERVER=${SERVER:-"wsl.exe -d Ubuntu -u root --"}
SERVER_REPO="${SERVER_REPO:-/mnt/d/kaggle/data-pipeline}"
PY="${PY:-.venv/Scripts/python.exe}"
export MSYS_NO_PATHCONV=1

server() { $SERVER bash -c "$1"; }
psql_data() { $SERVER bash "$SERVER_REPO/benchmarks/server_sql.sh" "$SERVER_REPO/$ENV_FILE" kaggle_challenge "$@"; }
reset() { $SERVER bash "$SERVER_REPO/benchmarks/reset_on_server.sh" "$SERVER_REPO/$ENV_FILE" > /dev/null 2>&1; }
expand() { "$PY" tools/expand_cli.py --env "$ENV_FILE" run --quiet "$@" 2>&1 | tail -4; }
state() {
  psql_data -qAt -c "SELECT 'job ' || status || ', tasks committed ' || (SELECT count(*) FROM pgdm_expansion.job_tasks t WHERE t.job_id = j.job_id AND t.state = 'done') || '/' || (SELECT count(*) FROM pgdm_expansion.job_tasks t WHERE t.job_id = j.job_id) || ', attempts ' || (SELECT count(*) FROM pgdm_expansion.job_attempts a WHERE a.job_id = j.job_id) || ', suspended fks/indexes ' || jsonb_array_length(suspended_fks) || '/' || jsonb_array_length(suspended_indexes) FROM pgdm_expansion.jobs j ORDER BY created_at DESC LIMIT 1"
}
# Interrupt only once the job is really running: at least $MIN_DONE tasks committed.
wait_mid_run() {
  local n
  for _ in $(seq 1 300); do
    n=$(psql_data -qAt -c "SELECT count(*) FROM pgdm_expansion.job_tasks WHERE state = 'done'" 2> /dev/null | tr -cd '0-9')
    [ "${n:-0}" -ge "$MIN_DONE" ] && return 0
  done
  echo "(the job never reached $MIN_DONE committed tasks)"
}
verify() {
  psql_data -q -f "$SERVER_REPO/benchmarks/verify.sql" 2>&1 | grep -E "FAIL|VERIFY|rows:"
  $SERVER bash "$SERVER_REPO/benchmarks/server_sql.sh" "$SERVER_REPO/$ENV_FILE" postgres_data_manager -qAt -c \
    "SELECT 'versions per destination: ' || string_agg(table_name || '=' || n, ', ' ORDER BY table_name) FROM (SELECT table_name, count(*) AS n FROM app_control.table_versions WHERE database_name = 'kaggle_challenge' AND table_name ~ '^table[1-6]\$' AND version_code <> '0000' GROUP BY 1) v"
}

manager_kill() {
  echo "== Manager / SSH session killed mid-run: the job must finish on the server on its own"
  reset
  "$PY" tools/expand_cli.py --env "$ENV_FILE" run --quiet > /dev/null 2>&1 &
  local pid=$!
  wait_mid_run; kill -9 "$pid" 2> /dev/null; echo "manager killed mid-run: $(state)"
  for _ in $(seq 1 120); do
    [ "$(psql_data -qAt -c "SELECT status FROM pgdm_expansion.jobs ORDER BY created_at DESC LIMIT 1")" = completed ] && break
    sleep 1
  done
  echo "server side, no manager attached: $(state)"
  echo "-- Expand again (must collect the finished job, not expand twice):"
  expand; verify
}

runner_kill() {
  echo "== Runner and every psql session killed -9 mid-run: Expand again must resume"
  reset
  "$PY" tools/expand_cli.py --env "$ENV_FILE" run --quiet > .local/recovery_first.log 2>&1 &
  wait_mid_run
  server "pkill -9 -x psql; pkill -9 -f '[r]unner.sh'; true"
  sleep 3; echo "after kill: $(state)"
  wait; tail -3 .local/recovery_first.log | cut -c1-300
  echo "-- Expand again (must resume and reuse the committed tasks):"
  expand; echo "after resume: $(state)"; verify
}

postgres_crash() {
  echo "== PostgreSQL crashed (immediate restart) mid-run: Expand again must resume"
  reset
  "$PY" tools/expand_cli.py --env "$ENV_FILE" run --quiet > .local/recovery_first.log 2>&1 &
  wait_mid_run
  server "sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D \$(sudo -u postgres psql -qAt -c 'show data_directory') -m immediate stop > /dev/null 2>&1; systemctl restart postgresql"
  sleep 4; echo "after crash: $(state)"
  wait; tail -2 .local/recovery_first.log | cut -c1-300
  echo "-- Expand again (must resume):"
  expand; echo "after resume: $(state)"; verify
}

abort() {
  echo "== Interrupted job discarded: destinations must return to their previous state"
  reset
  "$PY" tools/expand_cli.py --env "$ENV_FILE" run --quiet > /dev/null 2>&1 &
  wait_mid_run
  server "pkill -9 -x psql; pkill -9 -f '[r]unner.sh'; true"
  sleep 3; wait; echo "after kill: $(state)"
  "$PY" tools/expand_cli.py --env "$ENV_FILE" abort
  psql_data -qAt -c "SELECT 'rows left in destinations: ' || ((SELECT count(*) FROM public.table1) + (SELECT count(*) FROM public.table2) + (SELECT count(*) FROM public.table3) + (SELECT count(*) FROM public.table4) + (SELECT count(*) FROM public.table5) + (SELECT count(*) FROM public.table6)) || ', counters total ' || (SELECT sum(row_count) FROM public.pgdm_table_row_counts WHERE table_name ~ '^table[1-6]\$') || ', foreign keys ' || (SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND connamespace = 'public'::regnamespace) || ', indexes ' || (SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND tablename ~ '^table[1-6]\$')"
  echo "-- A fresh Expand after the discard:"
  expand; verify
}

case "$SCENARIO" in
  all) manager_kill; runner_kill; postgres_crash; abort ;;
  manager_kill|runner_kill|postgres_crash|abort) "$SCENARIO" ;;
  *) echo "unknown scenario $SCENARIO"; exit 2 ;;
esac
