#!/usr/bin/env bash
# Run the unmodified official generator directly on the Linux server
# (no SSH tunnel needed there).  usage: generate_on_server.sh <env-file> <rows> [extra generator args]
set -euo pipefail
ENV_FILE="${1:?env file}"; ROWS="${2:?rows}"; shift 2
set -a
# shellcheck disable=SC1090
. <(tr -d '\r' < "$ENV_FILE")
set +a
cd "$(dirname "$0")/.."
export PG_KAGGLE_CHALLENGE_PASS="$PGDM_PG_PASS"
exec python3 generate_database.py --rows "$ROWS" --host 127.0.0.1 \
    --postgres_port "${PGDM_PG_PORT:-5432}" --user "$PGDM_PG_USER" \
    --admin_database postgres "$@"
