"""Headless driver for the DB Manager's cross-table Expand.

Runs exactly the service-layer calls the GUI makes (same SSH connection, same
preflight, same report) so benchmarks and recovery tests can be scripted.

Connection settings come from an env file (KEY=VALUE lines) or the process
environment: PGDM_SSH_HOST, PGDM_SSH_PORT, PGDM_SSH_USER, PGDM_SSH_PASS,
PGDM_PG_PORT, PGDM_PG_USER, PGDM_PG_PASS.

    python tools/expand_cli.py --env .local/bench.env run
    python tools/expand_cli.py --env .local/bench.env status
    python tools/expand_cli.py --env .local/bench.env abort
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402


def load_env(path: str | None) -> dict:
    values = dict(os.environ)
    if path:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    return values


def connect(env: dict) -> PostgresAdminService:
    service = PostgresAdminService()
    service.connect(
        env["PGDM_SSH_HOST"],
        int(env.get("PGDM_SSH_PORT", 22)),
        env["PGDM_SSH_USER"],
        env["PGDM_SSH_PASS"],
        int(env.get("PGDM_PG_PORT", 5432)),
        env["PGDM_PG_USER"],
        env["PGDM_PG_PASS"],
    )
    return service


def load_destinations(recipe_dir: Path, title: str) -> list[dict]:
    destinations = []
    for path in sorted(recipe_dir.glob("*.sql")):
        match = re.fullmatch(r"\d+_(.+)\.sql", path.name)
        if not match:
            continue
        destinations.append(
            {
                "table_name": f"public.{match.group(1)}",
                "version_title": title,
                "sql": path.read_text(encoding="utf-8"),
            }
        )
    if not destinations:
        raise SystemExit(f"No NN_<table>.sql recipes found in {recipe_dir}")
    return destinations


def report_text(report: dict) -> tuple[str, float]:
    from ui.dialogs import ExpansionTimingReportDialog as Dialog

    return Dialog.build_report_text(report), Dialog.measured_total_seconds(report)


def cmd_run(service: PostgresAdminService, args) -> int:
    destinations = load_destinations(Path(args.recipes), args.title)
    raw_schemas = [item for item in args.raw_schemas.split(",") if item]
    report = service.create_cross_table_expansion_timing_report(
        args.database,
        args.source,
        raw_schemas,
        [item["table_name"] for item in destinations],
    )
    cancel_event = threading.Event()
    last = {"text": ""}

    def progress(stage, percent, message):
        text = f"[{stage} {float(percent):5.1f}%] {message}"
        if text != last["text"] and not args.quiet:
            print(text, flush=True)
        last["text"] = text

    wall_started = time.perf_counter()
    with service.expansion_timing_scope(
        report, "prepare", "Ensure control database metadata"
    ):
        service.ensure_admin_schema(cancel_event=cancel_event)
    result = service.execute_cross_table_expansion(
        database_name=args.database,
        source_full_table_name=args.source,
        raw_schemas=raw_schemas,
        destinations=destinations,
        cancel_event=cancel_event,
        progress_callback=progress,
        timing_report=report,
        requested_by=args.author,
        workstation_name=socket.gethostname(),
    )
    registered = service.register_cross_table_expansion_versions(
        expansion_result=result,
        requested_by=args.author,
        workstation_name=socket.gethostname(),
        progress_callback=progress,
        timing_report=report,
    )
    report["completed_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    text, total = report_text(report)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    if not args.quiet:
        print(text)
    print(
        json.dumps(
            {
                "measured_processing_total_s": round(total, 3),
                "client_wall_s": round(time.perf_counter() - wall_started, 3),
                "source_rows": report.get("source_rows"),
                "destination_rows": report.get("destination_rows"),
                "versions": {
                    item["table_name"]: item["version_code"] for item in registered
                },
            }
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--env", help="env file with PGDM_* connection settings")
    parser.add_argument("--database", default="kaggle_challenge")
    parser.add_argument("--source", default="public.raw_data")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run (or resume) the six-recipe expansion")
    run.add_argument("--recipes", default=str(ROOT / "SQL files"))
    run.add_argument("--raw-schemas", default="group001")
    run.add_argument("--title", default="benchmark expansion")
    run.add_argument("--author", default="benchmark")
    run.add_argument("--report", help="write the performance report to this file")
    run.add_argument("--quiet", action="store_true")
    sub.add_parser("status", help="show expansion jobs recorded on the server")
    sub.add_parser("abort", help="discard an unfinished expansion job")
    args = parser.parse_args()

    service = connect(load_env(args.env))
    try:
        if args.command == "run":
            return cmd_run(service, args)
        if args.command == "status":
            print(json.dumps(service.list_expansion_jobs(args.database), indent=2))
            return 0
        if args.command == "abort":
            print(json.dumps(service.abort_expansion_job(args.database, args.source)))
            return 0
        return 2
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())
