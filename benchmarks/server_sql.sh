#!/usr/bin/env bash
# Run psql on the benchmark server with the role from the env file.
# usage: server_sql.sh <env-file> <database> [psql args...]   (SQL on stdin or via -c/-f)
set -euo pipefail
ENV_FILE="${1:?env file}"; DB="${2:?database}"; shift 2
set -a
# shellcheck disable=SC1090
. <(tr -d '\r' < "$ENV_FILE")
set +a
export PGPASSWORD="$PGDM_PG_PASS"
exec psql -h 127.0.0.1 -p "${PGDM_PG_PORT:-5432}" -U "$PGDM_PG_USER" -d "$DB" -X -v ON_ERROR_STOP=1 "$@"
