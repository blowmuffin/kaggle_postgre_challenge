#!/usr/bin/env bash
# Return the challenge database to its freshly generated state (raw_data kept).
# usage: reset_on_server.sh <env-file>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${1:?env file}"
bash "$HERE/server_sql.sh" "$ENV_FILE" kaggle_challenge -q -f "$HERE/reset.sql" > /dev/null
bash "$HERE/server_sql.sh" "$ENV_FILE" kaggle_challenge -q -f "$HERE/repair_schema.sql" > /dev/null
bash "$HERE/server_sql.sh" "$ENV_FILE" "${PDM_CONTROL_DB:-postgres_data_manager}" -q -f "$HERE/reset_control.sql" > /dev/null
echo "reset done"
