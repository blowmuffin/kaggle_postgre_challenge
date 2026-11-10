#!/usr/bin/env bash
# Prepare a local benchmark server (PostgreSQL + sshd) on Debian/Ubuntu or WSL.
# Run as root:  bash benchmarks/setup_linux_server.sh <env-file>
# The env file supplies PGDM_SSH_USER, PGDM_SSH_PASS, PGDM_PG_USER, PGDM_PG_PASS
# and optionally PGDM_SSH_PORT (default 2222). Nothing secret is stored here.
set -euo pipefail

ENV_FILE="${1:?usage: setup_linux_server.sh <env-file>}"
set -a
# shellcheck disable=SC1090
. <(tr -d '\r' < "$ENV_FILE")
set +a
: "${PGDM_SSH_USER:?}" "${PGDM_SSH_PASS:?}" "${PGDM_PG_USER:?}" "${PGDM_PG_PASS:?}"
PGDM_SSH_PORT="${PGDM_SSH_PORT:-2222}"

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq postgresql openssh-server >/dev/null

id -u "$PGDM_SSH_USER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$PGDM_SSH_USER"
echo "$PGDM_SSH_USER:$PGDM_SSH_PASS" | chpasswd

mkdir -p /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/90-pgdm-bench.conf <<EOF
Port $PGDM_SSH_PORT
ListenAddress 127.0.0.1
PasswordAuthentication yes
AllowUsers $PGDM_SSH_USER
EOF
# Ubuntu >= 22.10 socket-activates sshd and would ignore Port above.
systemctl disable --now ssh.socket >/dev/null 2>&1 || true
systemctl enable ssh >/dev/null 2>&1 || true
systemctl restart ssh
systemctl enable --now postgresql >/dev/null 2>&1 || true

sudo -u postgres psql -v ON_ERROR_STOP=1 -qAt \
    -v role="$PGDM_PG_USER" -v pass="$PGDM_PG_PASS" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN CREATEDB PASSWORD %L', :'role', :'pass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec
SELECT format('ALTER ROLE %I LOGIN CREATEDB PASSWORD %L', :'role', :'pass') \gexec
SQL

echo "postgres: $(sudo -u postgres psql -qAt -c 'SHOW server_version')"
echo "sshd:     127.0.0.1:$PGDM_SSH_PORT user=$PGDM_SSH_USER"
echo "cores:    $(nproc)"
