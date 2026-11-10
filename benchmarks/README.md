# Reproducing the benchmarks and the recovery tests

Everything here drives the **same service-layer calls the GUI makes**
(`execute_cross_table_expansion` + `register_cross_table_expansion_versions`)
over the same SSH connection, through [`tools/expand_cli.py`](../tools/expand_cli.py).
The number it prints is the report's `MEASURED PROCESSING TOTAL`.

## Requirements

| Where | What |
|---|---|
| Windows (Manager) | Python 3.10+ (`pip install -r requirements.txt`), Git Bash for the helper scripts |
| Linux (server) | PostgreSQL 11+ (tested on 16.15) with `psql`, an SSH server, `bash`, `setsid`, `nohup`, `nproc` (coreutils/util-linux) |
| PostgreSQL role | The dedicated `LOGIN CREATEDB` role of the challenge. No superuser, no extension, no server configuration change |

`benchmarks/setup_linux_server.sh <env-file>` prepares a Debian/Ubuntu/WSL box
(installs PostgreSQL + sshd, creates the role and a login). Connection settings
live in an env file that is **not** committed:

```text
PGDM_SSH_HOST=127.0.0.1
PGDM_SSH_PORT=2222
PGDM_SSH_USER=...
PGDM_SSH_PASS=...
PGDM_PG_PORT=5432
PGDM_PG_USER=...
PGDM_PG_PASS=...
```

## Timing

```bash
# 1. official, unmodified generator (run it as documented, or directly on the server)
bash benchmarks/generate_on_server.sh <env-file> 1000000 --replace

# 2. reference run with the original engine, kept for comparison
bash benchmarks/bench.sh <env-file> legacy 1
bash benchmarks/server_sql.sh <env-file> kaggle_challenge -f benchmarks/snapshot_reference.sql

# 3. the new engine: each repetition resets, runs, prints the total and verifies
bash benchmarks/bench.sh <env-file> fast 3
```

`bench.sh` assumes the server is the local WSL distribution; for a remote
server set `SERVER="ssh root@host"` and `SERVER_REPO=/path/to/clone`.
The GUI path is identical: connect, open `raw_data`, **Expand**, six
destinations in file order, **Run expansion**; the report opens at the end.

## Correctness (`verify.sql`)

Compares all six destinations with the reference snapshot: every column of
every row and every foreign-key relationship, with surrogate identity values
mapped to natural keys (see the header of the file for why), plus constraint
definitions, index definitions, sequence settings, row counters and leftover
artifacts. The last line is `VERIFY PASS` or `VERIFY FAIL`.

## Failure recovery (`recovery_test.sh`)

```bash
bash benchmarks/recovery_test.sh <env-file> all     # or one scenario
```

| Scenario | What is done | Expected |
|---|---|---|
| `manager_kill` | the Manager process is killed mid-run | the job finishes on the server by itself; the next Expand collects it, nothing runs twice |
| `runner_kill` | the server-side runner and every `psql` are killed with `-9` | the next Expand resumes; committed tasks are reused |
| `postgres_crash` | `pg_ctl stop -m immediate` mid-run, then restart | the next Expand resumes |
| `abort` | interrupted job is discarded | destinations, counters, indexes and foreign keys are exactly as before the job |

Each scenario ends with `verify.sql` and the number of versions registered per
destination (must be exactly 1).

Manual controls:

```bash
python tools/expand_cli.py --env <env-file> status   # jobs, progress, attempts
python tools/expand_cli.py --env <env-file> run      # start / re-attach / resume / collect
python tools/expand_cli.py --env <env-file> abort    # discard the unfinished job
```

In the GUI, pressing **Expand** while a job is unfinished offers
**Resume / Discard / leave it**; the **Cancel** button of the progress window
discards the running job (same guarantee as the original rollback).

## Resource use

`sample_resources.sh start|stop` (on the server, as root) reports peak
PostgreSQL memory as PSS (shared buffers counted once), temp-file bytes and
WAL volume for one run.

## Engine switches

| Variable | Default | Meaning |
|---|---|---|
| `PDM_EXPAND_ENGINE` | `fast` | `legacy` runs the original single-transaction engine |
| `PDM_EXPAND_WORKERS` | cores (¾ of them above 4) | parallel sessions |
| `PDM_EXPAND_CHUNK_ROWS` | adaptive, 2,000–50,000 | source rows per checkpointed chunk |
| `PDM_EXPAND_FK_MODE` | `bulk` | `rowwise` keeps per-row foreign-key checks during the load |
| `PDM_EXPAND_INDEX_MODE` | `bulk` | `rowwise` keeps every index maintained during the load |
| `PDM_EXPAND_SEQUENCE_CACHE` | `256` | session-local identity cache during the load |
