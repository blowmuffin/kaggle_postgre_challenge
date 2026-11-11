"""Manager-side driver of the parallel, resumable Expand engine.

The Manager validates exactly as before, then hands the job to a detached
runner on the Linux server and only observes it. If the SSH link drops, the
job keeps running; running Expand again re-attaches, resumes, or collects the
finished result instead of expanding twice.

Environment switches (all optional):
    PDM_EXPAND_ENGINE=legacy     run the original single-transaction engine
    PDM_EXPAND_WORKERS=<n>       parallel sessions (default: server cores)
    PDM_EXPAND_CHUNK_ROWS=<n>    source rows per chunk (default: adaptive)
    PDM_EXPAND_FK_MODE=rowwise   keep per-row foreign-key checks during the load
"""
from __future__ import annotations

import json
import os
import shlex
import socket
import time
import uuid

from config import CONTROL_DB, CONTROL_SCHEMA, REMOTE_STORAGE_DIR
from services import expansion_engine as engine
from utils import split_table_name, sql_ident, sql_literal

_INTEGER_TYPES = {"bigint", "integer", "smallint"}
# SQLSTATE classes raised by the data or the recipe itself (constraint
# violation, bad cast, unknown column ...): retrying cannot succeed, so the
# job is rolled back at once, as the original single transaction would be.
_DETERMINISTIC_SQLSTATE_CLASSES = {"0A", "21", "22", "23", "2F", "38", "39", "3F", "42", "44", "P0"}
_CHUNK_WORK_MEM_FRACTION = 0.8   # share of work_mem one chunk's source rows may occupy


class FastExpansionMixin:
    # -- small helpers -------------------------------------------------------
    def _expansion_psql(self, database_name: str) -> str:
        return (
            f"psql -h localhost -U {shlex.quote(self.sql_username)} "
            f"-d {shlex.quote(database_name)} -X -qAt -v ON_ERROR_STOP=1 -f -"
        )

    def _expansion_json(self, database_name: str, sql: str, cancel_event=None) -> list:
        output = self.run_remote_command(
            self._expansion_psql(database_name), stdin_text=sql, cancel_event=cancel_event
        )
        return [
            json.loads(line)
            for line in output.splitlines()
            if line.strip()[:1] in ("{", "[")
        ]

    @staticmethod
    def _fast_expansion_config(nproc: int, probe: dict) -> dict:
        requested = (os.getenv("PDM_EXPAND_WORKERS") or "auto").strip().lower()
        # Default: every core on small servers; on larger ones leave a quarter
        # for the WAL writer, checkpointer and the hyper-threads that add little.
        workers = int(requested) if requested.isdigit() else (
            nproc if nproc <= 4 else nproc - nproc // 4
        )
        free = int(probe["max_connections"]) - int(probe["connections"]) - 5
        work_mem = int(probe.get("work_mem_bytes") or 0)
        row_bytes = int(probe.get("source_row_bytes") or 0)
        fit = (
            int(_CHUNK_WORK_MEM_FRACTION * work_mem / row_bytes)
            if work_mem > 0 and row_bytes > 0 else 50000
        )
        chunk_max = max(500, min(50000, fit))
        return {
            "workers": max(1, min(workers, 32, free)),
            "chunk_rows": int(os.getenv("PDM_EXPAND_CHUNK_ROWS") or 0),
            "chunk_min_rows": min(2000, chunk_max),
            "chunk_max_rows": chunk_max,
            "sequence_cache": int(os.getenv("PDM_EXPAND_SEQUENCE_CACHE") or 256),
            "fk_mode": (
                "rowwise"
                if (os.getenv("PDM_EXPAND_FK_MODE") or "").strip().lower() == "rowwise"
                else "bulk"
            ),
            "index_mode": (
                "rowwise"
                if (os.getenv("PDM_EXPAND_INDEX_MODE") or "").strip().lower() == "rowwise"
                else "bulk"
            ),
        }

    # -- registration SQL (same rows the original writes, made idempotent) ----
    def _build_fast_registration_sql(
        self,
        job_id,
        database_name,
        source_schema_name,
        source_table_name,
        raw_schemas,
        source_versions,
        destinations,
        requested_by,
        workstation_name,
    ) -> str:
        control = sql_ident(CONTROL_SCHEMA)
        versions_table = f"{control}.{sql_ident('table_versions')}"
        dependencies_table = f"{control}.{sql_ident('table_version_dependencies')}"
        registrations = f"{control}.{sql_ident('expansion_job_registrations')}"
        source_full = f"{source_schema_name}.{source_table_name}"
        canonical_versions = self._canonical_source_version_references(source_versions)
        lock_keys = sorted(
            f"{CONTROL_SCHEMA}.table_versions:{database_name}:"
            f"{item['schema_name']}:{item['pure_table_name']}"
            for item in destinations
        )
        statements = []
        for order, item in enumerate(destinations, start=1):
            extra = {
                "source_versions": canonical_versions,
                "raw_schemas": list(raw_schemas),
                "destination_columns": self._dedupe_non_empty_strings(
                    item.get("destination_columns")
                ),
                "destination_read_relations": [
                    {"schema_name": str(schema).strip(), "table_name": str(table).strip()}
                    for schema, table in (item.get("read_relations") or [])
                ],
            }
            notes = (
                f"Versioned expansion from {source_full}. Exact raw_schema scope: "
                + ", ".join(raw_schemas)
                + ". No additional dump was created."
            )
            history = self._build_server_version_history_expression(
                "next_version.version_code",
                item["version_title"],
                requested_by,
                workstation_name,
                item["sql_recipe"],
                notes,
            )
            statements.append(
                f"""
WITH next_version_number AS (
    SELECT COALESCE(MAX(version_code::integer) + 1, 0) AS value
    FROM {versions_table}
    WHERE database_name = {sql_literal(database_name)}
      AND schema_name = {sql_literal(item['schema_name'])}
      AND table_name = {sql_literal(item['pure_table_name'])}
),
next_version AS (
    SELECT CASE WHEN value < 10000 THEN lpad(value::text, 4, '0') ELSE value::text END
           AS version_code
    FROM next_version_number
),
inserted_version AS (
    INSERT INTO {versions_table} (
        database_name, schema_name, table_name, version_code, version_title,
        created_by, workstation_name, sql_recipe, restored_from_version,
        version_history_log, version_history_format, raw_dump_path, raw_hash,
        raw_ingested_at, raw_schema, operation_kind
    )
    SELECT {sql_literal(database_name)}, {sql_literal(item['schema_name'])},
           {sql_literal(item['pure_table_name'])}, next_version.version_code,
           {sql_literal(item['version_title'])}, {sql_literal(requested_by)},
           {sql_literal(workstation_name)}, {sql_literal(item['sql_recipe'])}, NULL,
           {history}, {sql_literal(self.VERSION_HISTORY_FORMAT_ENTRY)},
           NULL, NULL, NULL, NULL,
           {sql_literal(self.CROSS_TABLE_EXPANSION_OPERATION_KIND)}
    FROM next_version
    RETURNING id, version_code
),
inserted_dependency AS (
    INSERT INTO {dependencies_table} (
        table_version_id, dependency_kind, source_database_name,
        source_schema_name, source_table_name, dependency_payload
    )
    SELECT inserted_version.id,
           {sql_literal(self.CROSS_TABLE_EXPANSION_DEPENDENCY_KIND)},
           {sql_literal(database_name)}, {sql_literal(source_schema_name)},
           {sql_literal(source_table_name)},
           (SELECT manifest FROM pgdm_reg_manifest)
               || {sql_literal(json.dumps(extra, ensure_ascii=False, sort_keys=True))}::jsonb
    FROM inserted_version
    RETURNING table_version_id
)
INSERT INTO pgdm_reg (ord, table_name, version_code, dependency_count)
SELECT {order}, {sql_literal(item['table_name'])}, inserted_version.version_code,
       (SELECT COUNT(*) FROM inserted_dependency)
FROM inserted_version;
""".strip()
            )
        keys_sql = ", ".join(sql_literal(key) for key in lock_keys)
        job = sql_literal(job_id)
        return (
            "\\set ON_ERROR_STOP on\n"
            "\\set pgdm_manifest `cat manifest.json`\n"
            "SET client_min_messages = warning;\n"
            f"CREATE TABLE IF NOT EXISTS {registrations} (\n"
            "    job_id text PRIMARY KEY,\n"
            "    database_name text NOT NULL,\n"
            "    versions jsonb NOT NULL,\n"
            "    registered_at timestamptz NOT NULL DEFAULT clock_timestamp()\n"
            ");\n"
            f"SELECT EXISTS (SELECT 1 FROM {registrations} WHERE job_id = {job}) AS pgdm_done \\gset\n"
            "\\if :pgdm_done\n"
            "\\else\n"
            "BEGIN;\n"
            "CREATE TEMP TABLE pgdm_reg (ord integer, table_name text, version_code text,\n"
            "    dependency_count bigint) ON COMMIT DROP;\n"
            "CREATE TEMP TABLE pgdm_reg_manifest ON COMMIT DROP AS\n"
            "    SELECT :'pgdm_manifest'::jsonb AS manifest;\n"
            "DO $pgdm_reg$\n"
            "DECLARE v_key text;\n"
            "BEGIN\n"
            f"    FOREACH v_key IN ARRAY ARRAY[{keys_sql}]::text[] LOOP\n"
            "        PERFORM pg_advisory_xact_lock(hashtextextended(v_key, 0));\n"
            "    END LOOP;\n"
            "END\n"
            "$pgdm_reg$;\n"
            + "\n".join(statements)
            + "\n"
            # The job id row commits with the versions: exactly-once registration.
            f"INSERT INTO {registrations} (job_id, database_name, versions)\n"
            f"SELECT {job}, {sql_literal(database_name)}, jsonb_agg(jsonb_build_object(\n"
            "    'table_name', table_name, 'version_code', version_code,\n"
            "    'dependency_count', dependency_count) ORDER BY ord)\n"
            "FROM pgdm_reg;\n"
            "COMMIT;\n"
            "\\endif\n"
            f"SELECT versions::text FROM {registrations} WHERE job_id = {job};\n"
        )

    # -- the engine ----------------------------------------------------------
    def _run_fast_cross_table_expansion(
        self,
        database_name,
        prepared,
        destinations,
        source_versions,
        requested_by,
        workstation_name,
        cancel_event,
        progress_callback,
        post_commit_callback,
        timing_report,
    ):
        """Run the expansion with the parallel engine.

        Returns the result dict, or ``None`` when this recipe set must use the
        original engine (the reason is written to the report)."""
        from services.postgres_service import OperationCancelledError

        def fall_back(reason: str):
            self._append_expansion_timing(
                timing_report,
                "prepare",
                f"Parallel engine not used: {reason}",
                0.0,
                "note",
                include_in_total=False,
            )
            return None

        source_schema = prepared["source_schema_name"]
        source_table = prepared["source_table_name"]
        raw_schemas = list(prepared["raw_schemas"])
        requested_by = str(requested_by or "unknown").strip() or "unknown"
        workstation_name = str(workstation_name or socket.gethostname())

        with self.expansion_timing_scope(
            timing_report, "prepare", "Analyse recipes for parallel execution"
        ):
            plan_destinations, function_names = [], set()
            for item, original in zip(prepared["destinations"], destinations or []):
                statements = [
                    engine.analyze_statement(text)
                    for text in self._split_sql_statements(str(original.get("sql") or "").strip())
                    if self._tokenize_managed_sql(text)
                ]
                if len(statements) != len(item["statements"]):
                    return fall_back("a recipe could not be mapped to its statements")
                names = {str(name).lower() for _schema, name in item["function_references"]}
                function_names |= names
                plan_destinations.append(
                    engine.Destination(
                        schema=item["schema_name"],
                        table=item["pure_table_name"],
                        statements=statements,
                        prepared=list(item["statements"]),
                        reads={(str(s), str(t)) for s, t in item["read_relations"]},
                        functions=names,
                    )
                )

        with self.expansion_timing_scope(
            timing_report, "preflight", "Probe server capacity and existing expansion jobs"
        ):
            output = self.run_remote_command(
                'echo "PGDM_NPROC=$(nproc 2>/dev/null || echo 1)"; '
                + self._expansion_psql(database_name),
                stdin_text=engine.probe_sql(source_schema, source_table, function_names),
                cancel_event=cancel_event,
            )
        nproc, probe, jobs = 1, None, []
        for line in output.splitlines():
            line = line.strip()
            if line.startswith("PGDM_NPROC="):
                nproc = max(1, int(line.split("=", 1)[1] or 1))
            elif line.startswith("{"):
                probe = json.loads(line)
            elif line.startswith("["):
                jobs = json.loads(line)
        if not probe or not {"server_version_num", "max_connections", "connections"} <= set(probe):
            return fall_back("the server probe returned no usable answer")
        if int(probe["server_version_num"]) < 110000:
            return fall_back("PostgreSQL 11 or newer is required")

        raw_id = probe.get("raw_id") or {}
        chunkable = bool(raw_id.get("notnull")) and raw_id.get("type") in _INTEGER_TYPES
        job_id = uuid.uuid4().hex
        try:
            with self.expansion_timing_scope(
                timing_report, "prepare", "Build the parallel execution plan"
            ):
                plan = engine.build_plan(
                    job_id,
                    source_schema,
                    source_table,
                    raw_schemas,
                    plan_destinations,
                    set(probe.get("aggregates") or []),
                    chunkable,
                )
                signature = engine.job_signature(
                    database_name, source_schema, source_table, raw_schemas, plan_destinations
                )
                config = self._fast_expansion_config(nproc, probe)
        except engine.UnsupportedSql as exc:
            return fall_back(str(exc))

        active = [job for job in jobs if job["status"] in engine.ACTIVE_STATUSES]
        finished = [
            job for job in jobs if job["status"] == "completed" and job["signature"] == signature
        ]
        launch, resumed = True, False
        reuse = active[0] if active else finished[0] if finished else None
        if (
            reuse is not None
            and reuse["signature"] == signature
            and not reuse["alive"]
            # "running" without a live controller is a job whose sessions were killed
            and reuse["status"] in ("completed", "running", "interrupted", "failed", "created")
        ):
            # The destinations were emptied (TRUNCATE, restore) after the job
            # committed its rows: collecting or resuming it would be wrong data.
            with self.expansion_timing_scope(
                timing_report, "preflight", "Check earlier job against destination contents"
            ):
                check = self._expansion_json(
                    database_name,
                    engine.stale_job_sql(reuse["job_id"], [d.key for d in plan_destinations]),
                    cancel_event,
                )
            if check and check[0].get("stale"):
                if reuse["status"] == "completed":
                    with self.expansion_timing_scope(
                        timing_report, "preflight", "Acknowledge stale completed job"
                    ):
                        self._expansion_json(
                            database_name, engine.summary_sql(reuse["job_id"], True), cancel_event
                        )
                else:
                    try:
                        with self.expansion_timing_scope(
                            timing_report, "preflight", "Discard stale unfinished job"
                        ):
                            result = self._expansion_json(
                                database_name, engine.abort_sql(reuse["job_id"]), cancel_event
                            )
                    except OperationCancelledError:
                        raise
                    except Exception as exc:
                        result = [{"detail": str(exc)}]
                    if not result or result[0].get("status") != "aborted":
                        raise RuntimeError(
                            f"Expansion job {reuse['job_id']} refers to rows that are no longer "
                            "in the destination tables and could not be discarded automatically; "
                            "discard it manually (Expand > Discard, or 'python tools/expand_cli.py "
                            f"abort') and run Expand again.\n\n{(result or [{}])[0].get('detail') or ''}"
                        )
                active, finished = [], []
        if active:
            job = active[0]
            if job["signature"] != signature:
                raise RuntimeError(
                    f"An unfinished expansion job already exists for {source_schema}."
                    f"{source_table} (job {job['job_id']}, {job['status']}, "
                    f"{job['tasks_done']}/{job['tasks_total']} tasks committed) and it was "
                    "created with different recipes or scope. Resume it by running Expand "
                    "with its original recipes, or discard it first (Expand > Discard, or "
                    "'python tools/expand_cli.py abort')."
                )
            job_id, resumed, launch = job["job_id"], True, not job["alive"]
        elif finished:
            # The data and versions are already there; the previous Manager
            # session just never collected the result. Do not expand twice.
            job_id, resumed, launch = finished[0]["job_id"], True, False
        else:
            request = {
                "source_table_name": f"{source_schema}.{source_table}",
                "raw_schemas": raw_schemas,
                "requested_by": requested_by,
                "workstation_name": workstation_name,
                "destinations": [
                    {
                        "table_name": item.get("table_name"),
                        "version_title": item.get("version_title"),
                        "sql": item.get("sql"),
                    }
                    for item in destinations
                ],
            }
            try:
                with self.expansion_timing_scope(
                    timing_report, "prepare", "Create expansion job and validate generated SQL"
                ):
                    self.run_remote_command(
                        self._expansion_psql(database_name),
                        stdin_text=engine.create_job_sql(
                            {
                                "job_id": job_id,
                                "signature": signature,
                                "database": database_name,
                                "source_schema": source_schema,
                                "source_table": source_table,
                                "raw_schemas": raw_schemas,
                                "request": request,
                                "plan": plan,
                                "config": config,
                            }
                        ),
                        cancel_event=cancel_event,
                    )
            except OperationCancelledError:
                raise
            except Exception as exc:
                if "jobs_one_active_per_source" in str(exc):
                    raise RuntimeError(
                        "Another expansion job for this source was started at the same time."
                    ) from exc
                return fall_back(f"generated SQL was rejected ({str(exc).splitlines()[0][:200]})")

        def launch_runner():
            files = {
                "control.sql": engine.control_sql(
                    job_id, source_schema, source_table, raw_schemas, config, chunkable
                ),
                "locker.sql": engine.locker_sql(job_id, source_schema, source_table),
                "worker.sql": engine.worker_sql(job_id),
                "fail.sql": engine.fail_sql(job_id),
                "complete.sql": engine.complete_sql(job_id),
                "register.sql": self._build_fast_registration_sql(
                    job_id,
                    database_name,
                    source_schema,
                    source_table,
                    raw_schemas,
                    source_versions,
                    prepared["destinations"],
                    requested_by,
                    workstation_name,
                ),
                "runner.sh": engine.runner_script(
                    job_id, database_name, CONTROL_DB, self.sql_username, config["workers"]
                ),
            }
            with self.expansion_timing_scope(
                timing_report, "execute", "Launch detached server-side runner"
            ):
                self.run_remote_command(
                    "bash -s",
                    stdin_text=engine.launch_script(
                        f"{REMOTE_STORAGE_DIR}/expansion_jobs/{job_id}", files, job_id
                    ),
                    cancel_event=cancel_event,
                )

        if launch:
            launch_runner()
        self._notify_progress(
            progress_callback,
            "execute",
            2,
            ("Resuming" if resumed else "Running")
            + f" expansion job {job_id[:8]} on the server ({config['workers']} workers)...",
        )

        # Observe. Polls are not processing, so they are kept out of the report.
        waited_from = time.monotonic()
        polls, dead_since, relaunched, committed = 0, None, False, False
        while True:
            if cancel_event is not None and cancel_event.is_set() and not committed:
                with self.expansion_timing_scope(None, "execute", "Discard cancelled job"):
                    self.run_remote_command(
                        self._expansion_psql(database_name), stdin_text=engine.abort_sql(job_id)
                    )
                raise OperationCancelledError("Operacao cancelada pelo usuario.")
            try:
                with self.expansion_timing_scope(None, "execute", "Poll job"):
                    states = self._expansion_json(database_name, engine.status_sql(job_id))
            except Exception as exc:
                if cancel_event is not None and cancel_event.is_set() and not committed:
                    continue  # the Cancel button closed this poll; discard above
                raise RuntimeError(
                    f"Lost contact with the server while expansion job {job_id} was "
                    "running. The job itself keeps running there. Reconnect and click "
                    f"Expand again to re-attach to it.\n\n{exc}"
                ) from exc
            polls += 1
            if not states:
                raise RuntimeError(f"Expansion job {job_id} disappeared from the server.")
            state = states[0]
            status = state["status"]
            if status in ("data_complete", "completed") and not committed:
                committed = True
                if post_commit_callback:
                    post_commit_callback()
                self._notify_progress(
                    progress_callback, "execute", 100, "Data committed to the destinations."
                )
            if status == "completed":
                break
            if (
                status == "failed"
                and str(state.get("sqlstate") or "")[:2] in _DETERMINISTIC_SQLSTATE_CLASSES
            ):
                with self.expansion_timing_scope(None, "execute", "Roll back failed job"):
                    self.run_remote_command(
                        self._expansion_psql(database_name), stdin_text=engine.abort_sql(job_id)
                    )
                raise RuntimeError(
                    f"The expansion failed and was rolled back; the destinations are "
                    f"unchanged.\n\n{state.get('error')} (SQLSTATE {state.get('sqlstate')})"
                )
            if status in ("failed", "interrupted", "aborted"):
                raise RuntimeError(
                    f"Expansion job {job_id} is {status}: {state.get('error') or 'no detail'}\n\n"
                    f"{state['done']}/{state['total']} tasks are committed and kept. Run Expand "
                    "again with the same recipes to resume from there, or discard the job to "
                    "return the destinations to their previous state."
                )
            if state["alive"]:
                dead_since = None
            else:
                dead_since = dead_since or time.monotonic()
                if time.monotonic() - dead_since > 20:
                    if status == "data_complete" and not relaunched:
                        relaunched, dead_since = True, None
                        launch_runner()  # only version registration is left
                    else:
                        raise RuntimeError(
                            f"Expansion job {job_id} is no longer running on the server "
                            f"({state['done']}/{state['total']} tasks committed). Run Expand "
                            "again with the same recipes to resume it."
                        )
            if not committed and state["total"]:
                self._notify_progress(
                    progress_callback,
                    "execute",
                    5 + 90 * state["done"] / state["total"],
                    f"{state['done']}/{state['total']} tasks committed ({state['phase']}).",
                )
            time.sleep(0.15 if polls < 40 else 0.5 if polls < 200 else 1.5)

        with self.expansion_timing_scope(
            timing_report, "execute", "Collect job result and timing telemetry"
        ):
            summary = self._expansion_json(database_name, engine.summary_sql(job_id, True))[0]
        self._append_fast_expansion_timings(
            timing_report, summary, prepared, time.monotonic() - waited_from, polls
        )
        return {
            "engine": "fast",
            "job_id": job_id,
            "source_database_name": database_name,
            "source_schema_name": source_schema,
            "source_table_name": source_table,
            "raw_schemas": raw_schemas,
            "source_versions": source_versions,
            "dependency_manifest": summary.get("manifest") or {},
            "destinations": prepared["destinations"],
            "timing_report": timing_report,
            "registered_versions": list(summary.get("registered_versions") or []),
        }

    def _append_fast_expansion_timings(self, report, summary, prepared, waited, polls):
        config = summary.get("config") or {}
        for attempt in summary.get("attempts") or []:
            self._append_expansion_timing(
                report,
                "execute",
                (
                    f"Server-side expansion job, attempt {attempt['attempt_no']} "
                    f"({attempt.get('workers')} parallel sessions) - wall clock"
                ),
                float(attempt.get("seconds") or 0.0),
                "linux_server_wall",
                details=(
                    "Elapsed on the Linux server from runner launch to the attempt's last "
                    "commit: source lock, manifest, every chunk, constraint validation and "
                    f"version registration. Outcome: {attempt.get('outcome')}; "
                    f"{attempt.get('tasks')} tasks committed in this attempt."
                ),
            )
        labels = {
            stage["stage_no"]: stage["label"] for stage in summary.get("plan_stages") or []
        }
        for stage in summary.get("stages") or []:
            self._append_expansion_timing(
                report,
                "execute",
                f"Stage {stage['stage_no']}: "
                + labels.get(
                    stage["stage_no"],
                    "Rebuild suspended indexes"
                    if stage["stage_no"] == len(labels) + 1
                    else "Validate restored foreign keys",
                )
                + f" ({stage['tasks']} tasks)",
                float(stage.get("seconds") or 0.0),
                "linux_server_wall",
                details="First task start to last task commit; already inside the job wall clock.",
                include_in_total=False,
            )
        rows_by_destination = {}
        for item in summary.get("statements") or []:
            parts = str(item["key"]).split("|")
            seconds = float(item.get("seconds") or 0.0)
            if parts[0] == "validate":
                name, destination, rows = f"Validate foreign key {parts[2]} on {parts[1]}", parts[1], None
            elif parts[0] == "index":
                name, destination, rows = f"Build index {parts[2]} on {parts[1]}", parts[1], None
            elif parts[0] == "partial":
                name, destination, rows = f"Partial DISTINCT for {parts[1]}", parts[1], None
            else:
                destination, rows = parts[0], int(item.get("rows") or 0)
                name = f"Insert into {destination} (statement {parts[1]})"
                rows_by_destination[destination] = rows_by_destination.get(destination, 0) + rows
            self._append_expansion_timing(
                report,
                "execute",
                f"{name} - summed over {item['tasks']} parallel tasks",
                seconds,
                "postgresql_backend_wall_sum",
                rows=rows,
                destination=destination,
                details=(
                    "Sum of clock_timestamp() spans across the worker sessions (CPU-like); "
                    "already inside the job wall clock."
                ),
                include_in_total=False,
            )
        self._append_expansion_timing(
            report,
            "execute",
            f"Manager waited for the job ({polls} status polls)",
            waited,
            "local_wall",
            details="Observation only; the server-side wall clock above is the processing time.",
            include_in_total=False,
        )
        self._append_expansion_timing(
            report,
            "version",
            "Atomic, idempotent version registration by the server-side runner",
            0.0,
            "linux_server_wall",
            details=(
                "Performed inside the job (already in its wall clock); keyed by job id so a "
                f"retry cannot register twice. Foreign keys validated in bulk: "
                f"{summary.get('suspended_fks')}; indexes rebuilt in bulk: "
                f"{summary.get('suspended_indexes')}; engine config: {json.dumps(config)}."
            ),
            include_in_total=False,
        )
        manifest = summary.get("manifest") or {}
        report["source_rows"] = int(manifest.get("expected_row_count") or 0)
        report["destination_rows"] = [
            {
                "table_name": item["table_name"],
                "rows": int(rows_by_destination.get(item["table_name"], 0)),
            }
            for item in prepared["destinations"]
        ]
        report["total_rows_inserted"] = sum(item["rows"] for item in report["destination_rows"])

    # -- operator controls -----------------------------------------------------
    def list_expansion_jobs(self, database_name: str, cancel_event=None) -> list:
        result = self._expansion_json(database_name, engine.list_jobs_sql(), cancel_event)
        return result[0] if result else []

    def get_unfinished_expansion_job(
        self, database_name: str, source_full_table_name: str, cancel_event=None
    ):
        """The interrupted job of this source (with its original request), if any."""
        schema, table = split_table_name(source_full_table_name)
        result = self._expansion_json(
            database_name, engine.request_sql(schema, table), cancel_event
        )
        return result[0] if result else None

    def abort_expansion_job(
        self, database_name: str, source_full_table_name: str, cancel_event=None
    ) -> dict:
        """Discard the unfinished job of this source and undo what it inserted."""
        job = self.get_unfinished_expansion_job(database_name, source_full_table_name)
        if not job:
            return {"status": "none", "detail": "No unfinished expansion job."}
        result = self._expansion_json(
            database_name, engine.abort_sql(job["job_id"]), cancel_event
        )
        return result[0] if result else {"job_id": job["job_id"], "status": "unknown"}
