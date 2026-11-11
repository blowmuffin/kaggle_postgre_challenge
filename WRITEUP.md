# Kill it anywhere. Nothing is lost. And it is 7× faster.

**Expand as a parallel, checkpointed server-side job: same six recipes, same
output, row for row.**

Python & PostgreSQL Data Pipeline Optimization Challenge. Fork of
`igorsiecz/kaggle_postgre_challenge` at `5dfde9d`, Apache-2.0. The generator
and the six recipe files are byte-identical to upstream. AI coding assistants
were used. Every number here is a local measurement, not an official score.

> **Timeline, stated up front.** Commit `9b7c700` was public six hours before
> the deadline. This writeup and commit `6bb5a43` (one fix, told in §6) came
> after it. Both commits are measured below; judge whichever the rules allow.

## The scoreboard

Laptop: i5-13420H (4P+4E cores), 16 GB, Windows 11 Manager → SSH → WSL2
Ubuntu 24.04, PostgreSQL 16.15 with its **default configuration** and a
non-superuser role. Metric: the report's `MEASURED PROCESSING TOTAL`. Each run
is compared with a snapshot of the original engine's output
(`benchmarks/verify.sql`); all of them say `VERIFY PASS`.

| Source rows | Original | `9b7c700` (deadline) | `6bb5a43` (fix) | Best speed-up |
|---:|---:|---:|---:|---:|
| 100,000 | 8.0 – 8.2 s | 2.35 – 2.48 s | 2.14 – 2.22 s | 3.7× |
| 1,000,000 | 79 – 83 s | 12.9 – 14.0 s | 10.3 – 11.9 s | 8.1× |
| 3,000,000 | 262 – 265 s | 46.4 – 51.7 s | 37.4 – 49.2 s | 7.0× |

This laptop gives PostgreSQL about 4.4 cores of real throughput (12 identical
read-only backends each run 2.7× slower than one). The design is one session
per core, so the gap widens on a real server.

## 1. Where the original spends its time

Measured on the unmodified code, 100,000 rows, one backend.

| What it pays for | Evidence |
|---|---|
| **One backend.** `INSERT … SELECT` never gets a parallel plan | one core busy for all but 1 s of the 79 s at 1M rows |
| **Foreign keys checked row by row.** Each check is an index probe plus a `FOR KEY SHARE` lock; for `table1 → raw_data` that lock rewrites the header of every source row | `table1` insert: 995 ms with the checks, 213 ms without. WAL per million rows: 3.1 GB, against 0.77 GB here |
| **`LATERAL (VALUES …14 rows…)`** in the `table2` recipe evaluates 56 JSON extractions for every source row and rebuilds its expression state each time | 2,857 ms as written, 763 ms as `UNION ALL` branches |
| **Indexes maintained row by row** on tables that start empty | `table2`: 477 ms, against 157 ms heap + 138 ms bulk build |
| **All or nothing**, with the versions in a second transaction | an interruption loses everything, or leaves data without its version record |

## 2. What we built

```
Windows DB Manager ── SSH ──► Linux server
  preflight (unchanged validators)
  analyse recipes ─► plan
  create job  ───────────────► pgdm_expansion.jobs / job_tasks / job_attempts
  launch (setsid nohup) ─────► runner.sh ─┬─ controller : suspend FKs + indexes, manifest,
  poll one status row ◄──────  job tables │               task list, rebuild + validate
                                          ├─ locker     : SHARE lock on the source
                                          ├─ K workers  : claim a chunk (SKIP LOCKED), run it,
                                          │               counters, mark done, COMMIT
                                          └─ then       : register versions, mark completed
```

- **The recipes run verbatim.** Only `{{source}}` changes: the original
  substitutes `(SELECT * FROM raw_data WHERE raw_schema IN (…))`, a chunk adds
  `AND raw_id BETWEEN lo AND hi`. The registered `sql_recipe` is the original
  text; a diff of both engines' version rows shows no difference except the
  timestamp.
- **One PostgreSQL session per core, one chunk per transaction.** A chunk's
  rows, its row counters and its "done" mark commit together, so a chunk is
  applied exactly once or not at all.
- **Bulk instead of row-wise.** Foreign keys and secondary indexes of the
  loaded tables are recorded, dropped, and at the end rebuilt and validated by
  PostgreSQL itself (`CREATE INDEX`, `ADD CONSTRAINT … USING INDEX`,
  `VALIDATE CONSTRAINT`). The job cannot finish unless every one is back and
  valid; `verify.sql` checks the definitions character for character.
- **Detached.** The job runs on the server under `setsid nohup`. The Manager
  only watches it.
- **Python side.** `services/expansion_engine.py` (pure, unit-tested) lexes
  each recipe, proves whether it may be chunked, builds the staged plan and
  generates all SQL and shell text. `services/expansion_fast.py` orchestrates:
  preflight, probe, create, launch, poll, cancel, resume, discard. Anything
  the planner cannot prove safe runs through the original engine, unchanged.
- **One clock.** `MEASURED PROCESSING TOTAL` = the unchanged Manager-side items
  + the job's wall clock on the server from launch to last commit, summed over
  attempts. Lock, manifest, every chunk, rebuild, validation and version
  registration are inside it. Nothing is moved outside the measurement.

Stages for the official workload, derived from the recipes and not hard-coded:
partial `DISTINCT` per chunk for `table3`/`table4` → merge → chunked inserts
`table1 → table5 → table6 → table2` → index rebuild → foreign-key validation →
version registration.

## 3. Break it

State lives in the data database and commits with the work. Measured at 1M
rows unless noted; every row ends in `VERIFY PASS` and exactly one version per
table.

| You do this | It does this |
|---|---|
| Kill the Manager / drop SSH at 45 of 99 chunks | the job finishes on the server alone; the next Expand collects it in 2.6 s, nothing runs twice |
| `kill -9` the runner and every `psql` at 50 of 99 | the next Expand resumes as attempt 2 and reuses the 50 |
| `pg_ctl stop -m immediate` at 32 of 99 | the next Expand resumes |
| Kill the runner after the data phase | only version registration is re-run: exactly once, keyed by job id |
| Press Cancel mid-run | the job is discarded in about 4 s: 0 rows, counters 0, 6 FKs, 11 indexes back |
| Discard an interrupted job | rows deleted by the transaction ids of its committed chunks; state identical to before |
| Run Expand twice on the same data | duplicate key in a chunk → automatic rollback, first expansion intact |
| Empty the destinations after a job finished but was never collected (3M) | the stale job is retired, a fresh one runs |
| Empty the destinations after a job was killed mid-run (100k) | the dead job is discarded, constraints restored, a fresh one runs |

Re-run with `6bb5a43` at 3M rows, interrupted in the insert stage at about
2,540 of 3,665 chunks: Manager kill (the job finished alone, collected in
3.5 s), `kill -9` of every server process (resumed as attempt 2), PostgreSQL
crash (resumed as attempt 2) and discard (6,136,898 rows removed, then a fresh
35.9 s run). All four end in `VERIFY PASS` with one version per table.

The **Expand** button detects an unfinished job and offers Resume / Discard /
leave. `tools/expand_cli.py status|run|abort` does the same headlessly, and
`benchmarks/recovery_test.sh` reproduces the table. A resumed attempt refuses
to continue if the source changed in between.

## 4. Why the output is the same

- A recipe is chunked only if it is a single `INSERT … SELECT … FROM {{source}}`
  with no `GROUP BY`, `HAVING`, window, set operation, `ORDER BY`, `LIMIT`,
  sub-select, aggregate, or outer join that could cross chunks, and it does
  not read its own destination. Otherwise it runs once, whole, as before.
- It may read an earlier destination that is already complete, or one filled
  in the same chunk only through a join on the source's `raw_id`.
- `DISTINCT` of the union of per-chunk `DISTINCT`s is the global `DISTINCT`.
- Two mechanical rewrites: `LATERAL (VALUES …)` → `LATERAL (… UNION ALL …)`,
  and the chunk range repeated on a joined column that the join already
  implies. Every generated statement is planned with `EXPLAIN` at job
  creation; if PostgreSQL rejects one, the original engine is used.
- Kept: all source and destination validators, row counters, version rows and
  dependencies, "no additional dump" as in the original Expand.
- `verify.sql` compares every column of every row through natural keys, every
  foreign-key relationship, constraint and index definitions, sequences and
  counters.

## 5. The bill

Server side, one Expand, `6bb5a43`. Memory is the peak PSS of all PostgreSQL
processes (idle is about 145 MB); temp files from `pg_stat_database`.

| | Original, 1M | Ours, 1M | Original, 3M | Ours, 3M |
|---|---:|---:|---:|---:|
| Peak memory | 242 MB | 483 MB | 289 MB | 638 MB |
| Temp files | 75 – 136 MB | 210 MB | 440 MB | 1,190 MB |
| WAL | 3.1 GB | 0.77 GB | 9.3 GB | 2.3 GB |
| Extra tables | – | 1.4 MB | – | 3.8 MB |

We write a quarter of the WAL and make no staging copy of the data. We do
use about twice the memory and, at 3M rows, 2.7× the temp space. Memory in
the chunk phase equals the original's (227 – 292 MB); the peak is the final
phase, when up to nine sessions rebuild indexes. The temp files are those
index sorts (630 MB at 3M) and the validation joins (560 MB).
`PDM_EXPAND_INDEX_MODE=rowwise` trades them away for about 1.3× the time. No
memory setting is raised anywhere.

## 6. The bug that hid in the planner

Some runs were 5 – 10 s slower and wrote 4.8 GB of extra temp files at 3M
rows. Logging temp files per statement found it: the `table1` insert joins
each chunk to `table3`, which has four rows. Once `table3` had statistics,
PostgreSQL sometimes built the hash table from the **source** side — 50,000
rows of 1.6 KB — and spilled the whole chunk to disk.

The fix is not a planner hint. A chunk is now sized so that its source rows
fit in 80 % of `work_mem`: about 1,640 rows at the 4 MB default, measured from
`pg_class` or from 200 sampled rows if the table was never analysed. Whatever
the planner hashes or sorts inside a chunk stays in memory, and the later
statements of the same task re-read the chunk from cache. A partial index on
unfinished tasks keeps claiming cheap with thousands of chunks. Result at 1M
rows: insert stage 9 – 12 s → 6.7 – 7.3 s in most runs, and no spill in any.

`9b7c700` still has the 50,000-row cap and therefore this defect.

## 7. What it does not do

- While a job runs, and until an interrupted job is resumed or discarded, the
  destinations lack the suspended foreign keys and indexes, and committed
  chunks are visible. That is the price of keeping finished work.
- Destinations are not write-locked against other sessions; the source is.
- Identity values are handed out per session: not in source order, small gaps.
  Rows and relationships are identical. `PDM_EXPAND_WORKERS=1` gives order.
- Preflight still costs about 1 s of SSH round trips. Left alone on purpose:
  those are the validation guarantees.
- Run-to-run spread on this laptop is large (37 – 49 s at 3M). Nothing above
  3M rows was measured. The GUI prompt was read, not clicked through by hand.

## 8. Reproduce

```bash
python -m unittest discover -s tests                 # planner and sizing tests
bash benchmarks/bench.sh <env> fast 3                # timed, verified runs
bash benchmarks/recovery_test.sh <env> all           # the interruption table
```

`benchmarks/README.md` has setup, generation, the reference snapshot, resource
sampling and every engine switch.
