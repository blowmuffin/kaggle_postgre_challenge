#!/usr/bin/env bash
# Resource footprint of one Expand run, measured on the Linux server (as root).
#   sample_resources.sh start <dir>   -> begins sampling in the background
#   sample_resources.sh stop  <dir>   -> prints peak PostgreSQL memory (PSS, shared
#                                        pages counted once), temp-file bytes and WAL
set -u
MODE="${1:?start|stop}"; DIR="${2:?state dir}"   # mode "loop" is internal
q() { sudo -u postgres psql -qAt -d kaggle_challenge -c "$1"; }
pss_mb() {
  local total=0 v
  for pid in $(pgrep -x postgres); do
    v=$(awk '/^Pss:/ {print $2}' "/proc/$pid/smaps_rollup" 2> /dev/null || echo 0)
    total=$((total + ${v:-0}))
  done
  echo $((total / 1024))
}
if [ "$MODE" = start ]; then
  mkdir -p "$DIR"; rm -f "$DIR/stop"
  q "SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()" > "$DIR/temp0"
  q "SELECT pg_current_wal_lsn()" > "$DIR/lsn0"
  pss_mb > "$DIR/pss0"
  setsid nohup bash "$0" loop "$DIR" > /dev/null 2>&1 < /dev/null &
elif [ "$MODE" = loop ]; then
  peak=0
  while [ ! -e "$DIR/stop" ]; do
    v=$(pss_mb)
    if [ "$v" -gt "$peak" ]; then peak=$v; echo "$peak" > "$DIR/peak"; fi
    sleep 0.5
  done
else
  touch "$DIR/stop"; sleep 1
  temp1=$(q "SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()")
  wal=$(q "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '$(cat "$DIR/lsn0")'))")
  jobs=$(q "SELECT pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'pgdm_expansion' AND c.relkind = 'r'")
  echo "postgres_pss_idle_mb=$(cat "$DIR/pss0") postgres_pss_peak_mb=$(cat "$DIR/peak" 2> /dev/null || echo ?) temp_file_bytes=$((temp1 - $(cat "$DIR/temp0"))) wal_generated=$wal job_bookkeeping_tables=$jobs"
fi
