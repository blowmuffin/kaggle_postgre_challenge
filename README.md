# PostgreSQL Data Manager

PostgreSQL Data Manager is a desktop application for browsing and administering PostgreSQL databases through an SSH connection. It includes table previews, imports, version history, controlled destructive actions, data-dictionary tooling, dumps, restores, and role management.

## PostgreSQL setup

The application uses two different PostgreSQL objects:

- Control database: `postgres_data_manager` — the application creates it on the first connection when the PostgreSQL role has `CREATEDB`.
- Control schema: `app_control` — the application creates this automatically inside the control database, together with its internal tables.

For automatic setup, grant the PostgreSQL role permission to create databases once:

```sql
ALTER ROLE your_app_role CREATEDB;
```

If automatic creation is not allowed in your environment, run the following manually as a PostgreSQL administrator, replacing `your_app_role` with the PostgreSQL username entered in the connection window:

```sql
CREATE DATABASE postgres_data_manager OWNER your_app_role;
```

If the database already exists under another owner, grant the application role access instead:

```sql
GRANT CONNECT, CREATE ON DATABASE postgres_data_manager TO your_app_role;
```

The role must be able to connect to `postgres_data_manager` and create the `app_control` schema. The same `CREATEDB` permission also enables the optional database-creation button in the application.

The SSH server must provide the PostgreSQL command-line tools used by the application, including `psql`, `pg_dump`, and `pg_restore`.

## Configuration

Defaults can be overridden without changing the source code:

| Environment variable | Default | Purpose |
|---|---|---|
| `PDM_CONTROL_DB` | `postgres_data_manager` | Control database name |
| `PDM_CONTROL_SCHEMA` | `app_control` | Internal control schema |
| `PDM_LEGACY_CONTROL_SCHEMA` | `app_control_legacy` | Optional legacy schema inspected during migration |
| `PDM_REMOTE_STORAGE_DIR` | `postgres_data_manager_storage` | Storage directory under the SSH user's home |
| `PDM_LOCAL_STORAGE_DIR` | `PostgreSQL Data Manager` | Local application-data directory |
| `PDM_APP_TITLE` | `PostgreSQL Data Manager` | Window title |

Changing these variables does not rename existing PostgreSQL objects. Migrate or rename existing databases and schemas before pointing the application at new names.

## Local installation

Python 3.10 or newer is recommended.

```bash
python -m pip install -r requirements.txt
python main.py
```

On Windows, remembered credentials are encrypted for the current Windows user with DPAPI and stored under the configured local application-data directory.

## Synthetic challenge database

[`generate_database.py`](./generate_database.py) creates the deterministic `kaggle_challenge` database used to exercise the Manager with synthetic data. See [GENERATE_DATABASE_README.md](./GENERATE_DATABASE_README.md) for the Linux server setup, SSH tunnel command, resume behavior, generated metadata, and the six expansion recipes in [`SQL files`](./SQL%20files/).

## Expansion performance report

After a successful cross-table **Expand** operation, the application displays a detailed English-language performance report. It includes preparation and preflight work, destination locks, source-manifest scanning, every destination statement, affected-row counts, version registration, commits, cache invalidation, and interface refresh.

Local work is measured with per-thread CPU time. Remote command work is clocked on the Linux server, and expansion statements are clocked inside PostgreSQL. SSH connection and result-return transit are therefore excluded from the measured processing total. The report can be exported as a UTF-8 `.txt` file.

## Parallel, resumable Expand engine (challenge submission)

This fork replaces the single all-or-nothing Expand transaction with a job that
runs **on the Linux server**, in parallel, and survives interruptions. Nothing
changes in how Expand is used: same button, same six recipes, same report.
Design, measurements and trade-offs are in **[WRITEUP.md](./WRITEUP.md)**;
reproduction commands are in **[benchmarks/README.md](./benchmarks/README.md)**.

What it does, in short:

- The recipes are executed verbatim. Only the `{{source}}` relation is cut into
  `raw_id` ranges, and only for recipes where that provably cannot change the
  result; everything else runs once over the whole source as before.
- One PostgreSQL session per core commits one range per transaction, together
  with its row counters and a checkpoint row.
- Foreign keys and secondary indexes of the loaded tables are validated/built
  once, in bulk, by PostgreSQL itself, instead of row by row. The job cannot
  finish unless every one of them is back and valid.
- If the SSH link or the Manager dies, the job keeps running; if the server
  dies, the next **Expand** resumes from the committed ranges. An unfinished
  job can be **resumed or discarded** (rolled back) from the Expand button.
- Version registration is done by the job itself and is exactly-once.

Requirements beyond the original: PostgreSQL 11+, `bash`, `setsid`, `nohup`,
`nproc` on the server. No extension, no superuser, no configuration change.
The job keeps its bookkeeping in three small tables in schema `pgdm_expansion`
of the data database, one table `app_control.expansion_job_registrations` in
the control database, and its logs under
`~/postgres_data_manager_storage/expansion_jobs/<job id>/` on the server.

`PDM_EXPAND_ENGINE=legacy` runs the original engine unchanged; the other
switches are listed in [benchmarks/README.md](./benchmarks/README.md#engine-switches).

```bash
python -m unittest discover -s tests          # planner and timing tests
python tools/expand_cli.py --env <env> run    # headless Expand (same calls as the GUI)
bash benchmarks/recovery_test.sh <env> all    # interruption scenarios
```

## Main dependencies

- CustomTkinter
- Paramiko
- pandas
- ReportLab
- openpyxl and xlrd for Excel imports

## License

Licensed under the [Apache License 2.0](./LICENSE).
