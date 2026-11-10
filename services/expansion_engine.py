"""Planner and SQL generator for the parallel, resumable Expand engine.

Pure functions only (no I/O): recipe analysis, the stage plan, and the SQL /
shell text that runs on the Linux server. ``expansion_fast.py`` drives it.

Design in one paragraph: the six recipes are executed verbatim, but the
``{{source}}`` relation is restricted to a ``raw_id`` range so that K psql
workers can each commit one range ("chunk") per transaction. A recipe is only
chunked when that provably cannot change its result; anything else runs once
over the whole source exactly like the original engine.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

from utils import sql_ident, sql_literal

PLACEHOLDER = "{{source}}"
ENGINE_VERSION = 1
JOB_SCHEMA = "pgdm_expansion"
LO_MARK = "@@PGDM_LO@@"
HI_MARK = "@@PGDM_HI@@"
ROW_COUNTS_TABLE = '"public"."pgdm_table_row_counts"'
ACTIVE_STATUSES = ("created", "running", "interrupted", "failed", "data_complete")


class UnsupportedSql(Exception):
    """The fast engine cannot run this recipe set; use the original engine."""


# --------------------------------------------------------------------------
# Lexer
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Token:
    kind: str  # word | qident | string | number | op | punct | source
    text: str
    start: int
    end: int
    depth: int

    @property
    def low(self) -> str:
        return self.text.lower() if self.kind == "word" else self.text

    @property
    def name(self) -> str | None:
        if self.kind == "word":
            return self.text.lower()
        if self.kind == "qident":
            return self.text[1:-1].replace('""', '"')
        return None


_STRING_START = re.compile(r"(?:[eEbBxXnN]|[uU]&)?'")
_DOLLAR_TAG = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")
_PARAM = re.compile(r"\$\d+")
_NUMBER = re.compile(r"\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?")
_WORD = re.compile(r"[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*")
_OP_CHARS = frozenset("+-*/<>=~!@#%^&|`?")


def lex(sql: str) -> list[Token]:
    tokens: list[Token] = []
    i, n, depth = 0, len(sql), 0
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        if sql.startswith("/*", i):
            level, j = 1, i + 2
            while j < n and level:
                if sql.startswith("/*", j):
                    level, j = level + 1, j + 2
                elif sql.startswith("*/", j):
                    level, j = level - 1, j + 2
                else:
                    j += 1
            if level:
                raise UnsupportedSql("unterminated comment")
            i = j
            continue
        if sql.startswith(PLACEHOLDER, i):
            j = i + len(PLACEHOLDER)
            tokens.append(Token("source", PLACEHOLDER, i, j, depth))
            i = j
            continue
        match = _STRING_START.match(sql, i)
        if match:
            escapes = match.group(0)[0] in "eE"
            j = match.end()
            while True:
                if j >= n:
                    raise UnsupportedSql("unterminated string")
                if escapes and sql[j] == "\\":
                    j += 2
                elif sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            tokens.append(Token("string", sql[i:j], i, j, depth))
            i = j
            continue
        if c == '"':
            j = i + 1
            while True:
                if j >= n:
                    raise UnsupportedSql("unterminated identifier")
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            tokens.append(Token("qident", sql[i:j], i, j, depth))
            i = j
            continue
        if c == "$":
            match = _DOLLAR_TAG.match(sql, i)
            if match:
                j = sql.find(match.group(0), match.end())
                if j < 0:
                    raise UnsupportedSql("unterminated dollar quote")
                j += len(match.group(0))
                tokens.append(Token("string", sql[i:j], i, j, depth))
                i = j
                continue
            match = _PARAM.match(sql, i)
            if not match:
                raise UnsupportedSql("unexpected $")
            tokens.append(Token("number", match.group(0), i, match.end(), depth))
            i = match.end()
            continue
        match = _NUMBER.match(sql, i)
        if match:
            tokens.append(Token("number", match.group(0), i, match.end(), depth))
            i = match.end()
            continue
        match = _WORD.match(sql, i)
        if match:
            tokens.append(Token("word", match.group(0), i, match.end(), depth))
            i = match.end()
            continue
        if c == "(":
            tokens.append(Token("punct", c, i, i + 1, depth))
            depth += 1
            i += 1
            continue
        if c == ")":
            depth -= 1
            if depth < 0:
                raise UnsupportedSql("unbalanced parentheses")
            tokens.append(Token("punct", c, i, i + 1, depth))
            i += 1
            continue
        if c in ",;.[]":
            tokens.append(Token("punct", c, i, i + 1, depth))
            i += 1
            continue
        if c == ":":
            j = i + 2 if sql.startswith("::", i) else i + 1
            tokens.append(Token("op" if j == i + 2 else "punct", sql[i:j], i, j, depth))
            i = j
            continue
        if c in _OP_CHARS:
            j = i
            while (
                j < n
                and sql[j] in _OP_CHARS
                and not sql.startswith("--", j)
                and not sql.startswith("/*", j)
            ):
                j += 1
            tokens.append(Token("op", sql[i:j], i, j, depth))
            i = j
            continue
        raise UnsupportedSql(f"unexpected character {c!r}")
    if depth:
        raise UnsupportedSql("unbalanced parentheses")
    return tokens


def _is_name(token: Token) -> bool:
    return token.kind in ("word", "qident")


# --------------------------------------------------------------------------
# Recipe analysis
# --------------------------------------------------------------------------
_AFTER_RELATION = frozenset(
    "join inner left right full cross natural where on group order limit offset "
    "fetch union intersect except having window for using lateral".split()
)
_TOP_LEVEL_FORBIDDEN = frozenset(
    "group having window union intersect except order limit offset fetch for".split()
)
_JOIN_SIDES = frozenset("right full natural".split())
_ANYWHERE_FORBIDDEN = frozenset("over within filter tablesample".split())
_ON_CLAUSE_END = frozenset("join inner left right full cross natural where".split())


@dataclass
class Statement:
    sql: str
    tokens: list[Token] = field(default_factory=list)
    select_index: int = -1
    from_index: int = -1
    insert_columns: list[str] = field(default_factory=list)
    select_items: list[list[Token]] = field(default_factory=list)
    distinct: bool = False
    source_alias: str | None = None
    rowwise: bool = False
    reason: str = ""


def analyze_statement(sql: str) -> Statement:
    """Decide whether a recipe is row-wise in the source.

    Row-wise means: INSERT ... SELECT [DISTINCT] ... FROM {{source}} ..., with
    no construct that looks across source rows (grouping, windows, set
    operations, ordering, sub-selects, right/full joins). For such a statement
    the rows produced for a set of source rows are exactly the union of the
    rows produced for any partition of that set.
    """
    st = Statement(sql=sql.strip().rstrip(";").strip())

    def fail(reason: str) -> Statement:
        st.reason = reason
        return st

    try:
        tokens = lex(st.sql)
    except UnsupportedSql as exc:
        return fail(str(exc))
    st.tokens = tokens
    count = len(tokens)
    if count < 8 or tokens[0].low != "insert" or tokens[1].low != "into":
        return fail("not INSERT INTO")
    i = 2
    if not _is_name(tokens[i]):
        return fail("unreadable destination")
    i += 1
    if tokens[i].text == ".":
        i += 2
    if i >= count or tokens[i].text != "(" or tokens[i].depth != 0:
        return fail("no destination column list")
    i += 1
    columns: list[str] = []
    while i < count and not (tokens[i].text == ")" and tokens[i].depth == 0):
        if _is_name(tokens[i]):
            columns.append(tokens[i].name)
        elif tokens[i].text != ",":
            return fail("unreadable destination column list")
        i += 1
    i += 1
    if i >= count or tokens[i].low != "select" or tokens[i].depth != 0:
        return fail("no plain SELECT after the column list")
    st.select_index = i
    st.insert_columns = columns

    words = [token.low for token in tokens if token.kind == "word"]
    if words.count("select") != 1:
        return fail("nested SELECT")
    forbidden = _ANYWHERE_FORBIDDEN.intersection(words)
    if forbidden:
        return fail("uses " + ", ".join(sorted(forbidden)).upper())
    j = i + 1
    if j < count and tokens[j].low == "distinct":
        if j + 1 < count and tokens[j + 1].low == "on":
            return fail("DISTINCT ON")
        st.distinct = True
        j += 1
    if words.count("distinct") != (1 if st.distinct else 0):
        return fail("DISTINCT inside an expression")
    from_index = next(
        (k for k in range(j, count) if tokens[k].depth == 0 and tokens[k].low == "from"),
        -1,
    )
    if from_index < 0:
        return fail("no FROM")
    st.from_index = from_index

    items: list[list[Token]] = [[]]
    for token in tokens[j:from_index]:
        if token.depth == 0 and token.text == ",":
            items.append([])
        else:
            items[-1].append(token)
    if len(items) != len(columns) or not all(items):
        return fail("select list does not match the column list")
    st.select_items = items

    sources = sum(1 for token in tokens if token.kind == "source")
    if sources != 1 or from_index + 1 >= count or tokens[from_index + 1].kind != "source":
        return fail("the source must be the first FROM item, exactly once")
    k = from_index + 2
    if k < count and tokens[k].low == "as":
        k += 1
    if (
        k < count
        and _is_name(tokens[k])
        and tokens[k].depth == 0
        and tokens[k].low not in _AFTER_RELATION
    ):
        st.source_alias = tokens[k].name
    for index in range(from_index, count):
        token = tokens[index]
        if token.kind != "word" or token.depth != 0:
            continue
        following = tokens[index + 1].low if index + 1 < count else ""
        if token.low in _JOIN_SIDES:
            # RIGHT(...) / LEFT(...) are also string functions.
            if following in ("join", "outer"):
                return fail(f"{token.low.upper()} JOIN can emit rows without a source row")
        elif token.low in _TOP_LEVEL_FORBIDDEN:
            return fail(f"{token.low.upper()} at statement level")
    st.rowwise = True
    return st


def _raw_id_equality(conjunct: list[Token], alias: str, source_alias: str):
    """(column, start, end) of ``alias.column`` when the conjunct is exactly
    ``alias.column = source.raw_id`` (either order)."""
    if len(conjunct) != 7:
        return None
    if conjunct[3].text != "=" or conjunct[1].text != "." or conjunct[5].text != ".":
        return None
    names = [token.name if _is_name(token) else None for token in conjunct]
    if names[0] == alias and names[4] == source_alias and names[6] == "raw_id" and names[2]:
        return names[2], conjunct[0].start, conjunct[2].end
    if names[4] == alias and names[0] == source_alias and names[2] == "raw_id" and names[6]:
        return names[6], conjunct[4].start, conjunct[6].end
    return None


def relation_join(st: Statement, schema: str, table: str):
    """``(column, bounds)`` if *every* read of schema.table in ``st`` has the
    form ``JOIN schema.table [AS] a ON ... AND a.c = <source>.raw_id AND ...``,
    else ``None``. ``bounds`` lists ``(offset, "a.c")`` after each such term,
    where the chunk's raw_id range may be repeated for the planner."""
    tokens, count = st.tokens, len(st.tokens)
    if not st.rowwise or not st.source_alias:
        return None
    found: str | None = None
    bounds: list[tuple[int, str]] = []
    k = st.select_index
    while k < count - 2:
        hit = (
            _is_name(tokens[k])
            and tokens[k].name == schema
            and tokens[k + 1].text == "."
            and _is_name(tokens[k + 2])
            and tokens[k + 2].name == table
            and tokens[k - 1].text != "."
        )
        if not hit:
            k += 1
            continue
        if k < st.from_index or tokens[k].depth != 0 or tokens[k - 1].low != "join":
            return None
        m = k + 3
        if m < count and tokens[m].text == ".":
            return None
        if m < count and tokens[m].low == "as":
            m += 1
        if m >= count or not _is_name(tokens[m]) or tokens[m].low in _AFTER_RELATION:
            return None
        alias = tokens[m].name
        m += 1
        if m >= count or tokens[m].low != "on":
            return None
        m += 1
        end = m
        while end < count and not (
            tokens[end].depth == 0
            and (
                tokens[end].text == ","
                or (tokens[end].kind == "word" and tokens[end].low in _ON_CLAUSE_END)
            )
        ):
            end += 1
        clause = tokens[m:end]
        if any(t.depth == 0 and t.kind == "word" and t.low == "or" for t in clause):
            return None
        match = None
        current: list[Token] = []
        for token in clause + [None]:
            if token is None or (
                token.depth == 0 and token.kind == "word" and token.low == "and"
            ):
                if match is None:
                    match = _raw_id_equality(current, alias, st.source_alias)
                    if match:
                        bounds.append((current[-1].end, st.sql[match[1]:match[2]]))
                current = []
            else:
                current.append(token)
        if not match or (found and found != match[0]):
            return None
        found = match[0]
        k = end
    return (found, bounds) if found else None


def column_is_source_raw_id(st: Statement, column: str) -> bool:
    if not st.rowwise or not st.source_alias or column not in st.insert_columns:
        return False
    item = st.select_items[st.insert_columns.index(column)]
    return (
        len(item) == 3
        and _is_name(item[0])
        and item[0].name == st.source_alias
        and item[1].text == "."
        and _is_name(item[2])
        and item[2].name == "raw_id"
    )


def unroll_lateral_values(sql: str) -> str:
    """LATERAL (VALUES (r1), (r2) ...) AS a(cols) -> LATERAL (SELECT r1 UNION ALL SELECT r2 ...) AS a(cols).

    Same rows, same order and the same column types (VALUES resolves its column
    types with the UNION rules), but PostgreSQL can push the WHERE predicates
    into each branch and no longer rebuilds the expression state of every
    VALUES row for every source row. Anything not exactly this shape is left
    untouched.
    """
    try:
        tokens = lex(sql)
    except UnsupportedSql:
        return sql
    count = len(tokens)
    edits: list[tuple[int, int, str]] = []
    for i, token in enumerate(tokens):
        if not (
            token.kind == "word"
            and token.low == "lateral"
            and i + 2 < count
            and tokens[i + 1].text == "("
            and tokens[i + 2].kind == "word"
            and tokens[i + 2].low == "values"
        ):
            continue
        depth = tokens[i + 1].depth
        close = next(
            (k for k in range(i + 2, count) if tokens[k].text == ")" and tokens[k].depth == depth),
            None,
        )
        if close is None:
            continue
        rows: list[tuple[int, int]] = []
        k, well_formed = i + 3, True
        while k < close:
            if not (tokens[k].text == "(" and tokens[k].depth == depth + 1):
                well_formed = False
                break
            end = next(
                (e for e in range(k + 1, close) if tokens[e].text == ")" and tokens[e].depth == depth + 1),
                None,
            )
            if end is None or end == k + 1:
                well_formed = False
                break
            rows.append((tokens[k].end, tokens[end].start))
            k = end + 1
            if k < close:
                if tokens[k].text == "," and tokens[k].depth == depth + 1:
                    k += 1
                else:
                    well_formed = False
                    break
        alias_at = close + 1
        if alias_at < count and tokens[alias_at].low == "as":
            alias_at += 1
        has_column_aliases = (
            alias_at + 1 < count
            and _is_name(tokens[alias_at])
            and tokens[alias_at].low not in _AFTER_RELATION
            and tokens[alias_at + 1].text == "("
        )
        if not well_formed or len(rows) < 2 or not has_column_aliases:
            continue
        branches = "\n    UNION ALL ".join("SELECT " + sql[s:e].strip() for s, e in rows)
        edits.append((tokens[i + 2].start, tokens[close].start, branches + "\n"))
    for start, end, text in sorted(edits, reverse=True):
        sql = sql[:start] + text + sql[end:]
    return sql


# --------------------------------------------------------------------------
# Plan
# --------------------------------------------------------------------------
@dataclass
class Destination:
    schema: str
    table: str
    statements: list[Statement]
    prepared: list[str]  # the original engine's guarded statements
    reads: set[tuple[str, str]]
    functions: set[str]

    @property
    def key(self) -> tuple[str, str]:
        return (self.schema, self.table)

    @property
    def full(self) -> str:
        return f"{self.schema}.{self.table}"


def source_sql(schema: str, table: str, raw_schemas, chunked: bool) -> str:
    scope = ", ".join(sql_literal(value) for value in raw_schemas)
    bounds = (
        f"\n      AND {sql_ident('raw_id')} >= {LO_MARK}"
        f"\n      AND {sql_ident('raw_id')} <= {HI_MARK}"
        if chunked
        else ""
    )
    return (
        "(\n"
        "    SELECT *\n"
        f"    FROM {sql_ident(schema)}.{sql_ident(table)}\n"
        f"    WHERE {sql_ident('raw_schema')} IN ({scope}){bounds}\n"
        ")"
    )


def classify(dest: Destination, aggregate_names: set[str]) -> str:
    if dest.key in dest.reads:
        return "global"
    if dest.functions & aggregate_names:
        return "global"
    if not dest.statements or not all(st.rowwise for st in dest.statements):
        return "global"
    if any(st.distinct for st in dest.statements):
        if len(dest.statements) == 1 and not dest.reads:
            return "distinct"
        return "global"
    return "rowwise"


def _chunk_local(dest: Destination, other: Destination) -> bool:
    """dest may share a chunk with ``other`` (which it reads) only if every read
    is keyed on a column that ``other`` always fills with the source raw_id:
    then a chunk can only ever match rows produced from its own source rows
    (or rows that existed before the job)."""
    for st in dest.statements:
        if other.key not in _relations_in(st):
            continue
        join = relation_join(st, other.schema, other.table)
        if not join:
            return False
        if not all(column_is_source_raw_id(o, join[0]) for o in other.statements):
            return False
    return True


def _chunk_statement_sql(st: Statement, segment_dests: list[Destination], chunk_source: str) -> str:
    """The recipe, verbatim, on one raw_id range. Joins to destinations filled
    in the same chunk are keyed on raw_id, so the same range is restated on
    their side: without it the planner scans the whole (growing, un-analysed)
    destination for every chunk."""
    sql = st.sql
    edits: list[tuple[int, str]] = []
    present = _relations_in(st)
    for other in segment_dests:
        if other.key in present:
            join = relation_join(st, other.schema, other.table)
            if join:
                edits.extend(join[1])
    for offset, ref in sorted(edits, reverse=True):
        sql = (
            sql[:offset]
            + f" AND {ref} >= {LO_MARK} AND {ref} <= {HI_MARK}"
            + sql[offset:]
        )
    return unroll_lateral_values(sql).replace(PLACEHOLDER, chunk_source)


def _relations_in(st: Statement) -> set[tuple[str, str]]:
    found = set()
    tokens = st.tokens
    for k in range(max(st.select_index, 1), len(tokens) - 2):
        if (
            _is_name(tokens[k])
            and tokens[k + 1].text == "."
            and _is_name(tokens[k + 2])
            and tokens[k - 1].text != "."
        ):
            found.add((tokens[k].name, tokens[k + 2].name))
    return found


def build_plan(
    job_id: str,
    source_schema: str,
    source_table: str,
    raw_schemas,
    destinations: list[Destination],
    aggregate_names: set[str],
    chunkable_source: bool,
) -> dict:
    for dest in destinations:
        for text in dest.prepared:
            if LO_MARK in text or HI_MARK in text:
                raise UnsupportedSql("recipe contains a reserved marker")
    chunk_source = source_sql(source_schema, source_table, raw_schemas, True)
    full_source = source_sql(source_schema, source_table, raw_schemas, False)

    segments: list[dict] = []
    for dest in destinations:
        kind = classify(dest, aggregate_names)
        if not chunkable_source:
            kind = "global"
        last = segments[-1] if segments else None
        if kind == "rowwise":
            joins = last is not None and last["kind"] == "rowwise"
            if joins:
                for other in last["dests"]:
                    if other.key in dest.reads and not _chunk_local(dest, other):
                        joins = False
                        break
            if not joins:
                last = {"kind": "rowwise", "dests": []}
                segments.append(last)
            last["dests"].append(dest)
        elif kind == "distinct":
            if last is None or last["kind"] != "distinct":
                last = {"kind": "distinct", "dests": []}
                segments.append(last)
            last["dests"].append(dest)
        else:
            segments.append({"kind": "global", "dests": [dest]})

    stages: list[dict] = []
    setup: list[str] = []
    cleanup: list[str] = []
    partial_no = 0
    # Indexes whose leading column carries a chunk join must stay in place
    # during the load; every other index of a bulk-loaded table is rebuilt once.
    keep_index_columns: dict[str, list[str]] = {}
    for segment in segments:
        if segment["kind"] != "rowwise":
            continue
        for dest in segment["dests"]:
            for st in dest.statements:
                present = _relations_in(st)
                for other in segment["dests"]:
                    join = relation_join(st, other.schema, other.table) if other.key in present else None
                    if join and join[0] not in keep_index_columns.setdefault(other.full, []):
                        keep_index_columns[other.full].append(join[0])

    def add_stage(kind: str, label: str, statements: list[dict]):
        stages.append(
            {"stage_no": len(stages) + 1, "kind": kind, "label": label, "statements": statements}
        )

    def counted(dest: Destination, index: int, sql: str) -> dict:
        return {
            "sql": sql,
            "key": f"{dest.full}|{index}",
            "schema": dest.schema,
            "table": dest.table,
            "count": True,
            "explain": True,
        }

    for segment in segments:
        names = ", ".join(d.full for d in segment["dests"])
        if segment["kind"] == "rowwise":
            add_stage(
                "chunks",
                f"Parallel chunked inserts: {names}",
                [
                    counted(d, n, _chunk_statement_sql(st, segment["dests"], chunk_source))
                    for d in segment["dests"]
                    for n, st in enumerate(d.statements, start=1)
                ],
            )
        elif segment["kind"] == "distinct":
            partial_statements, final_statements = [], []
            for dest in segment["dests"]:
                st = dest.statements[0]
                partial_no += 1
                partial = f"{sql_ident(JOB_SCHEMA)}.{sql_ident(f'p_{job_id[:12]}_{partial_no}')}"
                select_start = st.tokens[st.select_index].start
                columns = ", ".join(f"c{n}" for n in range(1, len(st.insert_columns) + 1))
                select_sql = st.sql[select_start:]
                setup.append(
                    f"CREATE TABLE IF NOT EXISTS {partial} ({columns}) AS\n"
                    + select_sql.replace(PLACEHOLDER, full_source)
                    + "\nWITH NO DATA"
                )
                cleanup.append(f"DROP TABLE IF EXISTS {partial}")
                partial_statements.append(
                    {
                        "sql": f"INSERT INTO {partial}\n"
                        + select_sql.replace(PLACEHOLDER, chunk_source),
                        "key": f"partial|{dest.full}|1",
                        "count": False,
                        "explain": True,
                    }
                )
                final_statements.append(
                    counted(
                        dest,
                        1,
                        st.sql[:select_start] + f"SELECT DISTINCT {columns} FROM {partial}",
                    )
                )
                final_statements.append(
                    {"sql": f"DROP TABLE IF EXISTS {partial}", "key": None, "count": False, "explain": False}
                )
            add_stage("chunks", f"Parallel partial DISTINCT: {names}", partial_statements)
            add_stage("single", f"Merge DISTINCT: {names}", final_statements)
        else:
            dest = segment["dests"][0]
            add_stage(
                "single",
                f"Whole-source insert: {names}",
                [counted(dest, n, text) for n, text in enumerate(dest.prepared, start=1)],
            )

    return {
        "engine_version": ENGINE_VERSION,
        "source": {"schema": source_schema, "table": source_table},
        "destinations": [
            {"schema": d.schema, "table": d.table, "full": d.full} for d in destinations
        ],
        "setup": setup,
        "cleanup": cleanup,
        "keep_index_columns": keep_index_columns,
        "stages": stages,
        "chunked": any(stage["kind"] == "chunks" for stage in stages),
    }


def job_signature(database, source_schema, source_table, raw_schemas, destinations) -> str:
    payload = {
        "engine": ENGINE_VERSION,
        "database": database,
        "source": [source_schema, source_table],
        "raw_schemas": list(raw_schemas),
        "destinations": [[d.full, d.prepared] for d in destinations],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


# --------------------------------------------------------------------------
# Server-side SQL
# --------------------------------------------------------------------------
SCHEMA_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {JOB_SCHEMA};
CREATE TABLE IF NOT EXISTS {JOB_SCHEMA}.jobs (
    job_id text PRIMARY KEY,
    signature text NOT NULL,
    engine_version integer NOT NULL,
    status text NOT NULL DEFAULT 'created',
    phase text NOT NULL DEFAULT 'created',
    database_name text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    raw_schemas jsonb NOT NULL,
    request jsonb NOT NULL,
    plan jsonb NOT NULL,
    config jsonb NOT NULL,
    manifest jsonb,
    start_xid bigint,
    suspended_fks jsonb NOT NULL DEFAULT '[]'::jsonb,
    sequence_cache jsonb NOT NULL DEFAULT '[]'::jsonb,
    suspended_indexes jsonb NOT NULL DEFAULT '[]'::jsonb,
    empty_destinations jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    cancel_requested boolean NOT NULL DEFAULT false,
    error_message text,
    error_sqlstate text,
    registered_versions jsonb,
    acknowledged boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    data_completed_at timestamptz,
    completed_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_per_source
    ON {JOB_SCHEMA}.jobs (source_schema, source_table)
    WHERE status IN ('created', 'running', 'interrupted', 'failed', 'data_complete');
CREATE TABLE IF NOT EXISTS {JOB_SCHEMA}.job_tasks (
    job_id text NOT NULL REFERENCES {JOB_SCHEMA}.jobs ON DELETE CASCADE,
    stage_no integer NOT NULL,
    task_no integer NOT NULL,
    lo bigint,
    hi bigint,
    state text NOT NULL DEFAULT 'pending',
    statements jsonb,
    rows jsonb,
    seconds jsonb,
    xid bigint,
    attempt_no integer,
    started_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (job_id, stage_no, task_no)
) WITH (fillfactor = 50);
CREATE TABLE IF NOT EXISTS {JOB_SCHEMA}.job_attempts (
    job_id text NOT NULL REFERENCES {JOB_SCHEMA}.jobs ON DELETE CASCADE,
    attempt_no integer NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text,
    workers integer,
    PRIMARY KEY (job_id, attempt_no)
);
""".strip()

_SESSION_SETUP = """
SET client_min_messages = warning;
SELECT set_config('client_connection_check_interval', '1000', false)
WHERE current_setting('server_version_num')::int >= 140000;
""".strip()


_JOB_ID = re.compile(r"[0-9a-f]{32}")


def checked_job_id(job_id) -> str:
    """Job ids are embedded in SQL and shell text; accept only our own format,
    including when the id was read back from the server."""
    if not isinstance(job_id, str) or not _JOB_ID.fullmatch(job_id):
        raise UnsupportedSql("invalid expansion job id")
    return job_id


# The job tables hold SQL that the sessions EXECUTE. Refuse to run unless they
# belong to the connected role and nobody else may write to them.
OWNERSHIP_GUARD = f"""
DO $pgdm_guard$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n
        WHERE n.nspname = '{JOB_SCHEMA}'
          AND NOT pg_catalog.pg_has_role(n.nspowner, 'USAGE'))
       OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{JOB_SCHEMA}' AND c.relkind = 'r'
          AND NOT pg_catalog.pg_has_role(c.relowner, 'USAGE'))
       OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE n.nspname = '{JOB_SCHEMA}' AND c.relkind = 'r'
          AND a.privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'TRIGGER', 'REFERENCES')
          AND (a.grantee = 0 OR NOT pg_catalog.pg_has_role(a.grantee, 'USAGE')))
       OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute t
        JOIN pg_catalog.pg_class c ON c.oid = t.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(t.attacl) a
        WHERE n.nspname = '{JOB_SCHEMA}' AND c.relkind = 'r'
          AND a.privilege_type IN ('INSERT', 'UPDATE', 'REFERENCES')
          AND (a.grantee = 0 OR NOT pg_catalog.pg_has_role(a.grantee, 'USAGE')))
       OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger g
        JOIN pg_catalog.pg_class c ON c.oid = g.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{JOB_SCHEMA}' AND NOT g.tgisinternal)
    THEN
        RAISE EXCEPTION 'The {JOB_SCHEMA} job tables are owned or writable by another role; refusing to execute their stored statements.';
    END IF;
END
$pgdm_guard$;
""".strip()


def _lock_key(job_id: str) -> str:
    return f"hashtextextended({sql_literal('pgdm_expansion_job:' + job_id)}, 0)"


def create_job_sql(job: dict) -> str:
    job_id = sql_literal(checked_job_id(job["job_id"]))
    return f"""
\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL client_min_messages = warning;
{SCHEMA_DDL}
INSERT INTO {JOB_SCHEMA}.jobs (
    job_id, signature, engine_version, database_name, source_schema,
    source_table, raw_schemas, request, plan, config
) VALUES (
    {job_id}, {sql_literal(job['signature'])}, {ENGINE_VERSION},
    {sql_literal(job['database'])}, {sql_literal(job['source_schema'])},
    {sql_literal(job['source_table'])},
    {sql_literal(json.dumps(list(job['raw_schemas'])))}::jsonb,
    {sql_literal(json.dumps(job['request'], ensure_ascii=False))}::jsonb,
    {sql_literal(json.dumps(job['plan'], ensure_ascii=False))}::jsonb,
    {sql_literal(json.dumps(job['config']))}::jsonb
);
DO $pgdm_create$
DECLARE
    v_plan jsonb;
    v_sql text;
    v_stage jsonb;
    v_stmt jsonb;
BEGIN
    SELECT plan INTO v_plan FROM {JOB_SCHEMA}.jobs WHERE job_id = {job_id};
    FOR v_sql IN SELECT jsonb_array_elements_text(v_plan -> 'setup') LOOP
        EXECUTE v_sql;
    END LOOP;
    -- Plan every generated statement once: a rewrite that PostgreSQL rejects
    -- aborts job creation and the Manager falls back to the original engine.
    FOR v_stage IN SELECT * FROM jsonb_array_elements(v_plan -> 'stages') LOOP
        FOR v_stmt IN SELECT * FROM jsonb_array_elements(v_stage -> 'statements') LOOP
            IF (v_stmt ->> 'explain')::boolean THEN
                EXECUTE 'EXPLAIN ' || replace(replace(
                    v_stmt ->> 'sql', '{LO_MARK}', '0'), '{HI_MARK}', '0');
            END IF;
        END LOOP;
    END LOOP;
END
$pgdm_create$;
COMMIT;
""".strip() + "\n"


def control_sql(job_id: str, source_schema: str, source_table: str, raw_schemas, config: dict, chunkable: bool) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    src = f"{sql_ident(source_schema)}.{sql_ident(source_table)}"
    src_lit = sql_literal(src)
    scope = ", ".join(sql_literal(value) for value in raw_schemas)
    workers = int(config["workers"])
    chunk_rows = int(config.get("chunk_rows") or 0)
    chunk_min = int(config.get("chunk_min_rows", 2000))
    chunk_max = int(config.get("chunk_max_rows", 50000))
    seq_cache = int(config.get("sequence_cache") or 1) if workers > 1 else 1
    raw_id = sql_ident("raw_id") if chunkable else "NULL::bigint"
    # These two are embedded in a format() template below.
    src_fmt, scope_fmt = src.replace("%", "%%"), scope.replace("%", "%%")
    wait_block = f"""
DO $pgdm_ctl$
DECLARE
    v_status text;
    v_cancel boolean;
    v_seen timestamptz := clock_timestamp();
    v_tick integer := 0;
BEGIN
    LOOP
        SELECT status, cancel_requested INTO v_status, v_cancel
        FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
        IF v_status <> 'running' THEN
            RAISE EXCEPTION 'PGDM_STOP: the job is %', v_status;
        END IF;
        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM {JOB_SCHEMA}.job_tasks WHERE job_id = {j} AND state <> 'done'
        );
        IF v_cancel THEN
            UPDATE {JOB_SCHEMA}.jobs
            SET status = 'interrupted', error_message = 'Cancelled on request.'
            WHERE job_id = {j};
            COMMIT;
            RAISE EXCEPTION 'PGDM_STOP: cancelled';
        END IF;
        v_tick := v_tick + 1;
        IF v_tick % 50 = 0 THEN
            PERFORM pg_stat_clear_snapshot();
            IF EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE application_name = {sql_literal('pgdm_expand_worker:' + job_id)}
            ) THEN
                v_seen := clock_timestamp();
            ELSIF clock_timestamp() - v_seen > interval '15 seconds' THEN
                UPDATE {JOB_SCHEMA}.jobs
                SET status = 'interrupted', error_message = 'All worker sessions were lost.'
                WHERE job_id = {j};
                COMMIT;
                RAISE EXCEPTION 'PGDM_STOP: workers lost';
            END IF;
        END IF;
        PERFORM pg_sleep(0.005);
    END LOOP;
END
$pgdm_ctl$;
""".strip()
    return f"""
\\set ON_ERROR_STOP on
SET application_name = {sql_literal('pgdm_expand_control:' + job_id)};
{_SESSION_SETUP}
SET lock_timeout = '300s';
SELECT set_config('pgdm.launch_ts', :'launch_ts', false);
{OWNERSHIP_GUARD}
SELECT pg_try_advisory_lock({_lock_key(job_id)}) AS pgdm_got \\gset
\\if :pgdm_got
\\else
\\echo PGDM_BUSY
\\quit
\\endif
SELECT status = 'data_complete' AS pgdm_skip FROM {JOB_SCHEMA}.jobs WHERE job_id = {j} \\gset
\\if :pgdm_skip
\\quit
\\endif

-- 1. Open the attempt. First attempt only: suspend the destinations' foreign
--    keys; they are re-created and validated in bulk before the job can
--    reach data_complete (step 4), or by Abort.
DO $pgdm_ctl$
DECLARE
    j {JOB_SCHEMA}.jobs%ROWTYPE;
    v_attempt integer;
    v_dest jsonb;
    v_fk record;
    v_seq record;
    v_idx record;
    v_indexes jsonb := '[]'::jsonb;
    v_bulk jsonb := '[]'::jsonb;
    v_sequences jsonb := '[]'::jsonb;
    v_suspended jsonb := '[]'::jsonb;
    v_empty jsonb := '{{}}'::jsonb;
    v_is_empty boolean;
    v_existing bigint;
    v_source_rows double precision;
BEGIN
    PERFORM set_config('search_path', 'pg_catalog', true);
    SELECT * INTO j FROM {JOB_SCHEMA}.jobs WHERE job_id = {j} FOR UPDATE;
    IF j.status NOT IN ('created', 'running', 'interrupted', 'failed') THEN
        RAISE EXCEPTION 'Expansion job % is % and cannot be started.', j.job_id, j.status;
    END IF;
    UPDATE {JOB_SCHEMA}.job_attempts a
    SET finished_at = GREATEST(a.started_at, COALESCE((
            SELECT max(t.finished_at) FROM {JOB_SCHEMA}.job_tasks t
            WHERE t.job_id = a.job_id AND t.attempt_no = a.attempt_no), a.started_at)),
        outcome = COALESCE(a.outcome, 'interrupted')
    WHERE a.job_id = {j} AND a.finished_at IS NULL;
    SELECT COALESCE(max(attempt_no), 0) + 1 INTO v_attempt
    FROM {JOB_SCHEMA}.job_attempts WHERE job_id = {j};
    INSERT INTO {JOB_SCHEMA}.job_attempts (job_id, attempt_no, started_at, workers)
    VALUES ({j}, v_attempt,
            to_timestamp(current_setting('pgdm.launch_ts')::double precision), {workers});
    IF j.status = 'created' THEN
        SELECT GREATEST(reltuples, 0) INTO v_source_rows
        FROM pg_class WHERE oid = {src_lit}::regclass;
        FOR v_dest IN SELECT * FROM jsonb_array_elements(j.plan -> 'destinations') LOOP
            EXECUTE format('SELECT NOT EXISTS (SELECT 1 FROM %I.%I)',
                           v_dest ->> 'schema', v_dest ->> 'table') INTO v_is_empty;
            v_empty := v_empty || jsonb_build_object(v_dest ->> 'full', v_is_empty);
            -- Parallel sessions would otherwise queue on the single sequence
            -- page for every row. A session-local cache removes that; the
            -- original value is put back in step 4 (or by Abort).
            FOR v_seq IN
                SELECT s.seqrelid::regclass::text AS name, s.seqcache
                FROM pg_depend d
                JOIN pg_sequence s ON s.seqrelid = d.objid
                JOIN pg_class r ON r.oid = d.refobjid
                JOIN pg_namespace n ON n.oid = r.relnamespace
                WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
                  AND d.deptype IN ('a', 'i') AND d.refobjsubid > 0
                  AND n.nspname = v_dest ->> 'schema' AND r.relname = v_dest ->> 'table'
                  AND s.seqcache < {seq_cache} AND pg_has_role(r.relowner, 'USAGE')
            LOOP
                EXECUTE format('ALTER SEQUENCE %s CACHE %s', v_seq.name, {seq_cache});
                v_sequences := v_sequences || jsonb_build_array(jsonb_build_object(
                    'name', v_seq.name, 'cache', v_seq.seqcache));
            END LOOP;
            SELECT COALESCE(max(row_count), 0) INTO v_existing
            FROM {ROW_COUNTS_TABLE}
            WHERE schema_name = v_dest ->> 'schema' AND table_name = v_dest ->> 'table';
            -- Bulk validation / index builds re-read the whole table, so they
            -- only pay off when it is not already much larger than this load.
            CONTINUE WHEN NOT v_is_empty AND v_existing > v_source_rows;
            v_bulk := v_bulk || jsonb_build_array(v_dest);
            CONTINUE WHEN (j.config ->> 'fk_mode') <> 'bulk';
            FOR v_fk IN
                SELECT c.conname, pg_get_constraintdef(c.oid) AS definition,
                       obj_description(c.oid, 'pg_constraint') AS comment
                FROM pg_constraint c
                JOIN pg_class r ON r.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = r.relnamespace
                WHERE c.contype = 'f' AND c.convalidated AND c.conparentid = 0
                  AND c.coninhcount = 0 AND r.relkind = 'r'
                  AND n.nspname = v_dest ->> 'schema' AND r.relname = v_dest ->> 'table'
                  AND pg_has_role(r.relowner, 'USAGE')
                ORDER BY c.conname
            LOOP
                EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                               v_dest ->> 'schema', v_dest ->> 'table', v_fk.conname);
                v_suspended := v_suspended || jsonb_build_array(jsonb_build_object(
                    'schema', v_dest ->> 'schema', 'table', v_dest ->> 'table',
                    'name', v_fk.conname, 'definition', v_fk.definition,
                    'comment', v_fk.comment));
            END LOOP;
        END LOOP;
        -- Second pass, once no suspended foreign key depends on them: drop the
        -- secondary indexes of the bulk-loaded tables. They are rebuilt in
        -- parallel from sorted data in step 4 (uniqueness is enforced by that
        -- build), which costs far less CPU and WAL than per-row maintenance.
        FOR v_dest IN SELECT * FROM jsonb_array_elements(v_bulk) LOOP
            CONTINUE WHEN (j.config ->> 'index_mode') <> 'bulk';
            FOR v_idx IN
                SELECT i.indexrelid::regclass::text AS qualified_name, ic.relname AS index_name,
                       pg_get_indexdef(i.indexrelid) AS definition, c.conname, c.contype,
                       obj_description(c.oid, 'pg_constraint') AS constraint_comment,
                       obj_description(i.indexrelid, 'pg_class') AS index_comment
                FROM pg_index i
                JOIN pg_class ic ON ic.oid = i.indexrelid
                JOIN pg_am am ON am.oid = ic.relam
                JOIN pg_class r ON r.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = r.relnamespace
                LEFT JOIN pg_constraint c
                       ON c.conindid = i.indexrelid AND c.conrelid = i.indrelid
                      AND c.contype IN ('p', 'u', 'x')
                WHERE n.nspname = v_dest ->> 'schema' AND r.relname = v_dest ->> 'table'
                  AND r.relkind = 'r' AND am.amname = 'btree'
                  AND i.indisvalid AND i.indisready AND i.indislive
                  AND NOT i.indisreplident AND NOT i.indisclustered
                  AND pg_has_role(r.relowner, 'USAGE')
                  AND (c.oid IS NULL OR (
                        c.contype IN ('p', 'u') AND NOT c.condeferrable
                        AND c.conname = ic.relname AND c.coninhcount = 0 AND c.conparentid = 0))
                  AND NOT EXISTS (SELECT 1 FROM pg_constraint f
                                  WHERE f.contype = 'f' AND f.conindid = i.indexrelid)
                  AND NOT EXISTS (SELECT 1 FROM pg_inherits h WHERE h.inhrelid = i.indexrelid)
                  AND NOT COALESCE((
                        SELECT a.attname::text FROM pg_attribute a
                        WHERE a.attrelid = i.indrelid AND a.attnum = i.indkey[0]) = ANY (ARRAY(
                        SELECT jsonb_array_elements_text(COALESCE(
                            j.plan -> 'keep_index_columns' -> (v_dest ->> 'full'), '[]'::jsonb)))),
                        false)
                ORDER BY ic.relname
            LOOP
                IF v_idx.conname IS NOT NULL THEN
                    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
                                   v_dest ->> 'schema', v_dest ->> 'table', v_idx.conname);
                ELSE
                    EXECUTE format('DROP INDEX %s', v_idx.qualified_name);
                END IF;
                v_indexes := v_indexes || jsonb_build_array(jsonb_build_object(
                    'schema', v_dest ->> 'schema', 'table', v_dest ->> 'table',
                    'index', v_idx.index_name, 'definition', v_idx.definition,
                    'constraint', v_idx.conname, 'contype', v_idx.contype,
                    'constraint_comment', v_idx.constraint_comment,
                    'index_comment', v_idx.index_comment));
            END LOOP;
        END LOOP;
        UPDATE {JOB_SCHEMA}.jobs
        SET status = 'running', phase = 'locking', suspended_fks = v_suspended,
            suspended_indexes = v_indexes,
            sequence_cache = v_sequences,
            empty_destinations = v_empty, start_xid = txid_current(), error_message = NULL
        WHERE job_id = {j};
    ELSE
        UPDATE {JOB_SCHEMA}.jobs
        SET status = 'running', phase = 'locking', error_message = NULL,
            error_sqlstate = NULL, cancel_requested = false
        WHERE job_id = {j};
    END IF;
END
$pgdm_ctl$;

-- 2. With the source write-locked by the locker session: build the dependency
--    manifest (first attempt) or prove the source is unchanged (resume), then
--    release the task list to the workers.
DO $pgdm_ctl$
DECLARE
    j {JOB_SCHEMA}.jobs%ROWTYPE;
    v_waiting_since timestamptz := clock_timestamp();
    v_pending boolean;
    v_changed_sql text := 'false';
    v_manifest jsonb;
    v_missing text;
    v_total bigint;
    v_lo bigint;
    v_hi bigint;
    v_changed bigint;
    v_target bigint;
    v_width bigint;
    v_stage jsonb;
BEGIN
    LOOP
        -- pg_stat_activity is snapshotted per transaction; look afresh each time.
        PERFORM pg_stat_clear_snapshot();
        EXIT WHEN EXISTS (
            SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid = a.pid
            WHERE a.application_name = {sql_literal('pgdm_expand_locker:' + job_id)}
              AND l.locktype = 'relation' AND l.relation = {src_lit}::regclass
              AND l.mode = 'ShareLock' AND l.granted
        );
        IF clock_timestamp() - v_waiting_since > interval '300 seconds' THEN
            RAISE EXCEPTION 'The source table could not be locked within 300 s.';
        END IF;
        PERFORM pg_sleep(0.003);
    END LOOP;
    SELECT * INTO j FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
    v_pending := j.manifest IS NULL OR EXISTS (
        SELECT 1 FROM {JOB_SCHEMA}.job_tasks WHERE job_id = {j} AND state <> 'done');
    IF v_pending THEN
        IF j.manifest IS NOT NULL THEN
            v_changed_sql := format('age(xmin) <= age(%L::xid)',
                                    (j.start_xid & 4294967295)::text);
        END IF;
        EXECUTE format($sql$
            CREATE TEMP TABLE pgdm_manifest ON COMMIT DROP AS
            SELECT {sql_ident('raw_schema')}::text AS raw_schema,
                   {sql_ident('raw_hash')}::text AS raw_hash,
                   count(*)::bigint AS row_count,
                   min({raw_id}) AS lo, max({raw_id}) AS hi,
                   count(*) FILTER (WHERE %s) AS changed
            FROM {src_fmt}
            WHERE {sql_ident('raw_schema')} IN ({scope_fmt})
            GROUP BY 1, 2$sql$, v_changed_sql);
        SELECT string_agg(e.raw_schema, ', ' ORDER BY e.raw_schema) INTO v_missing
        FROM unnest(ARRAY[{scope}]::text[]) AS e(raw_schema)
        WHERE NOT EXISTS (SELECT 1 FROM pgdm_manifest a WHERE a.raw_schema = e.raw_schema);
        IF v_missing IS NOT NULL THEN
            RAISE EXCEPTION 'raw_schema has no source rows: %.', v_missing;
        END IF;
        SELECT COALESCE(sum(row_count), 0), min(lo), max(hi), COALESCE(sum(changed), 0)
        INTO v_total, v_lo, v_hi, v_changed FROM pgdm_manifest;
        v_manifest := jsonb_build_object(
            'raw_schemas', to_jsonb(ARRAY[{scope}]::text[]),
            'expected_row_count', v_total,
            'expected_distinct_raw_hash_count',
                (SELECT count(*) FROM (SELECT DISTINCT raw_hash FROM pgdm_manifest) d),
            'raw_hash_manifest', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'raw_schema', raw_schema, 'raw_hash', raw_hash, 'row_count', row_count)
                    ORDER BY raw_schema, raw_hash NULLS FIRST)
                FROM pgdm_manifest), '[]'::jsonb),
            'source_columns', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'name', attname, 'type', format_type(atttypid, atttypmod))
                    ORDER BY attnum)
                FROM pg_attribute
                WHERE attrelid = {src_lit}::regclass AND attnum > 0 AND NOT attisdropped),
                '[]'::jsonb));
        IF j.manifest IS NULL THEN
            v_target := {chunk_rows};
            IF v_target <= 0 THEN
                v_target := GREATEST({chunk_min},
                                     LEAST({chunk_max}, v_total / ({workers} * 4)));
            END IF;
            FOR v_stage IN SELECT * FROM jsonb_array_elements(j.plan -> 'stages') LOOP
                IF v_stage ->> 'kind' = 'chunks' THEN
                    v_width := GREATEST(1, ceil(
                        (v_hi - v_lo + 1)::numeric * v_target / GREATEST(v_total, 1)))::bigint;
                    INSERT INTO {JOB_SCHEMA}.job_tasks (job_id, stage_no, task_no, lo, hi)
                    SELECT {j}, (v_stage ->> 'stage_no')::integer, g,
                           v_lo + (g - 1) * v_width, LEAST(v_lo + g * v_width - 1, v_hi)
                    FROM generate_series(1, ceil((v_hi - v_lo + 1)::numeric / v_width)::bigint) g;
                ELSE
                    INSERT INTO {JOB_SCHEMA}.job_tasks (job_id, stage_no, task_no)
                    VALUES ({j}, (v_stage ->> 'stage_no')::integer, 1);
                END IF;
            END LOOP;
            UPDATE {JOB_SCHEMA}.jobs SET manifest = v_manifest WHERE job_id = {j};
        ELSIF v_changed > 0 OR v_manifest IS DISTINCT FROM j.manifest THEN
            UPDATE {JOB_SCHEMA}.jobs
            SET status = 'failed',
                error_message = 'The source changed after this job started, so its '
                    'committed chunks cannot be combined with the remaining ones. '
                    'Discard the job (Abort) and run the expansion again.'
            WHERE job_id = {j};
            COMMIT;
            RAISE EXCEPTION 'PGDM_STOP: source changed since the job started';
        END IF;
    END IF;
    UPDATE {JOB_SCHEMA}.jobs SET phase = 'running' WHERE job_id = {j};
END
$pgdm_ctl$;

-- 3. Wait for the workers to finish every stage.
{wait_block}

-- 4a. Release the source lock, restore the sequence settings and let the
--     workers rebuild the suspended indexes, one index per session.
DO $pgdm_ctl$
DECLARE
    j {JOB_SCHEMA}.jobs%ROWTYPE;
    v_fk jsonb;
    v_stage integer;
BEGIN
    UPDATE {JOB_SCHEMA}.jobs SET phase = 'restoring' WHERE job_id = {j};
    COMMIT;
    SELECT * INTO j FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
    v_stage := jsonb_array_length(j.plan -> 'stages') + 1;
    FOR v_fk IN SELECT * FROM jsonb_array_elements(j.sequence_cache) LOOP
        EXECUTE format('ALTER SEQUENCE %s CACHE %s', v_fk ->> 'name', v_fk ->> 'cache');
    END LOOP;
    INSERT INTO {JOB_SCHEMA}.job_tasks (job_id, stage_no, task_no, statements)
    SELECT {j}, v_stage, x.n::integer, jsonb_build_array(jsonb_build_object(
               'sql', x.f ->> 'definition',
               'key', format('index|%s.%s|%s', x.f ->> 'schema', x.f ->> 'table', x.f ->> 'index'),
               'count', false))
    FROM jsonb_array_elements(j.suspended_indexes) WITH ORDINALITY AS x(f, n)
    WHERE to_regclass(format('%I.%I', x.f ->> 'schema', x.f ->> 'index')) IS NULL
    ON CONFLICT (job_id, stage_no, task_no) DO UPDATE
        SET state = 'pending', statements = EXCLUDED.statements;
    UPDATE {JOB_SCHEMA}.jobs SET phase = 'indexing' WHERE job_id = {j};
END
$pgdm_ctl$;

{wait_block}

-- 4b. Re-attach the PRIMARY KEY / UNIQUE constraints to the rebuilt indexes,
--     put the suspended foreign keys back (NOT VALID, instant) and let the
--     workers validate them, one table per session.
DO $pgdm_ctl$
DECLARE
    j {JOB_SCHEMA}.jobs%ROWTYPE;
    v_fk jsonb;
    v_stage integer;
BEGIN
    UPDATE {JOB_SCHEMA}.jobs SET phase = 'restoring' WHERE job_id = {j};
    COMMIT;
    SELECT * INTO j FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
    v_stage := jsonb_array_length(j.plan -> 'stages') + 2;
    FOR v_fk IN SELECT * FROM jsonb_array_elements(j.suspended_indexes) LOOP
        IF v_fk ->> 'constraint' IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = v_fk ->> 'schema' AND r.relname = v_fk ->> 'table'
              AND c.conname = v_fk ->> 'constraint')
        THEN
            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s USING INDEX %I',
                           v_fk ->> 'schema', v_fk ->> 'table', v_fk ->> 'constraint',
                           CASE v_fk ->> 'contype' WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                           v_fk ->> 'index');
            IF v_fk ->> 'constraint_comment' IS NOT NULL THEN
                EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
                               v_fk ->> 'constraint', v_fk ->> 'schema', v_fk ->> 'table',
                               v_fk ->> 'constraint_comment');
            END IF;
        END IF;
        IF v_fk ->> 'index_comment' IS NOT NULL THEN
            EXECUTE format('COMMENT ON INDEX %I.%I IS %L', v_fk ->> 'schema',
                           v_fk ->> 'index', v_fk ->> 'index_comment');
        END IF;
    END LOOP;
    FOR v_fk IN SELECT * FROM jsonb_array_elements(j.suspended_fks) LOOP
        CONTINUE WHEN EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = v_fk ->> 'schema' AND r.relname = v_fk ->> 'table'
              AND c.conname = v_fk ->> 'name');
        EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s NOT VALID',
                       v_fk ->> 'schema', v_fk ->> 'table', v_fk ->> 'name',
                       v_fk ->> 'definition');
        IF v_fk ->> 'comment' IS NOT NULL THEN
            EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
                           v_fk ->> 'name', v_fk ->> 'schema', v_fk ->> 'table',
                           v_fk ->> 'comment');
        END IF;
    END LOOP;
    INSERT INTO {JOB_SCHEMA}.job_tasks (job_id, stage_no, task_no, statements)
    SELECT {j}, v_stage, t.task_no, t.statements
    FROM (
        SELECT dense_rank() OVER (ORDER BY f ->> 'schema', f ->> 'table')::integer AS task_no,
               jsonb_agg(jsonb_build_object(
                   'sql', format('ALTER TABLE %I.%I VALIDATE CONSTRAINT %I',
                                 f ->> 'schema', f ->> 'table', f ->> 'name'),
                   'key', format('validate|%s.%s|%s',
                                 f ->> 'schema', f ->> 'table', f ->> 'name'),
                   'count', false) ORDER BY f ->> 'name')
                   FILTER (WHERE NOT c.convalidated) AS statements
        FROM jsonb_array_elements(j.suspended_fks) f
        JOIN pg_namespace n ON n.nspname = f ->> 'schema'
        JOIN pg_class r ON r.relnamespace = n.oid AND r.relname = f ->> 'table'
        JOIN pg_constraint c ON c.conrelid = r.oid AND c.conname = f ->> 'name'
        GROUP BY f ->> 'schema', f ->> 'table'
    ) t
    WHERE t.statements IS NOT NULL
    ON CONFLICT (job_id, stage_no, task_no) DO UPDATE
        SET state = 'pending', statements = EXCLUDED.statements;
    UPDATE {JOB_SCHEMA}.jobs SET phase = 'validating' WHERE job_id = {j};
END
$pgdm_ctl$;

{wait_block}

-- 5. Data is complete only when every suspended constraint is back and valid.
DO $pgdm_ctl$
DECLARE
    v_missing text;
BEGIN
    SELECT string_agg(f ->> 'name', ', ') INTO v_missing
    FROM {JOB_SCHEMA}.jobs job, jsonb_array_elements(job.suspended_fks) f
    WHERE job.job_id = {j} AND NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = f ->> 'schema' AND r.relname = f ->> 'table'
          AND c.conname = f ->> 'name' AND c.convalidated);
    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'Foreign keys were not restored: %', v_missing;
    END IF;
    SELECT string_agg(f ->> 'index', ', ') INTO v_missing
    FROM {JOB_SCHEMA}.jobs job, jsonb_array_elements(job.suspended_indexes) f
    WHERE job.job_id = {j} AND (
        NOT EXISTS (
            SELECT 1 FROM pg_index i
            WHERE i.indexrelid = to_regclass(format('%I.%I', f ->> 'schema', f ->> 'index'))
              AND i.indisvalid AND i.indisready)
        OR (f ->> 'constraint' IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = f ->> 'schema' AND r.relname = f ->> 'table'
              AND c.conname = f ->> 'constraint')));
    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'Indexes or key constraints were not restored: %', v_missing;
    END IF;
    UPDATE {JOB_SCHEMA}.jobs
    SET status = 'data_complete', phase = 'data_complete',
        data_completed_at = clock_timestamp()
    WHERE job_id = {j} AND status = 'running';
END
$pgdm_ctl$;
""".strip() + "\n"


def locker_sql(job_id: str, source_schema: str, source_table: str) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    src = f"{sql_ident(source_schema)}.{sql_ident(source_table)}"
    return f"""
\\set ON_ERROR_STOP on
SET application_name = {sql_literal('pgdm_expand_locker:' + job_id)};
{_SESSION_SETUP}
-- Holds SHARE on the source (readers allowed, writers blocked) for as long as
-- chunks are being processed, so every chunk sees the same source.
DO $pgdm_lock$
DECLARE
    v_phase text;
    v_status text;
    v_locked boolean := false;
    v_started timestamptz := clock_timestamp();
    v_tick integer := 0;
BEGIN
    LOOP
        SELECT phase, status INTO v_phase, v_status
        FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
        EXIT WHEN v_status IS NULL OR v_status IN ('data_complete', 'completed', 'aborted');
        IF NOT v_locked THEN
            IF v_phase = 'locking' AND v_status = 'running' THEN
                LOCK TABLE {src} IN SHARE MODE;
                v_locked := true;
            END IF;
        ELSIF v_status <> 'running' OR v_phase NOT IN ('locking', 'running') THEN
            EXIT;
        END IF;
        v_tick := v_tick + 1;
        IF v_tick % 100 = 0 AND clock_timestamp() - v_started > interval '10 seconds' THEN
            PERFORM pg_stat_clear_snapshot();
            EXIT WHEN NOT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE application_name = {sql_literal('pgdm_expand_control:' + job_id)});
        END IF;
        PERFORM pg_sleep(0.003);
    END LOOP;
END
$pgdm_lock$;
""".strip() + "\n"


def worker_sql(job_id: str) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    return f"""
SET application_name = {sql_literal('pgdm_expand_worker:' + job_id)};
{_SESSION_SETUP}
SET synchronous_commit = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;
SELECT set_config('enable_memoize', 'off', false)
WHERE current_setting('server_version_num')::int >= 140000;
\\set ON_ERROR_STOP on
{OWNERSHIP_GUARD}
\\set ON_ERROR_STOP off
DO $pgdm_worker$
DECLARE
    v_status text;
    v_phase text;
    v_cancel boolean;
    v_plan jsonb;
    v_attempt integer;
    v_stage_no integer;
    v_task {JOB_SCHEMA}.job_tasks%ROWTYPE;
    v_statements jsonb;
    v_stmt jsonb;
    v_sql text;
    v_rows jsonb;
    v_seconds jsonb;
    v_count bigint;
    v_t0 timestamptz;
    v_started timestamptz;
    v_idle integer := 0;
    v_boot timestamptz := clock_timestamp();
BEGIN
    LOOP
        SELECT status, phase, plan INTO v_status, v_phase, v_plan
        FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
        EXIT WHEN v_status = 'running'
              AND v_phase IN ('running', 'restoring', 'indexing', 'validating');
        IF v_status IS NULL OR v_status IN ('data_complete', 'completed', 'aborted') THEN
            RETURN;
        END IF;
        v_idle := v_idle + 1;
        IF v_idle % 100 = 0 AND clock_timestamp() - v_boot > interval '10 seconds' THEN
            PERFORM pg_stat_clear_snapshot();
            IF NOT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE application_name = {sql_literal('pgdm_expand_control:' + job_id)})
            THEN
                RETURN;
            END IF;
        END IF;
        COMMIT;  -- never sit on locks or a snapshot while waiting
        PERFORM pg_sleep(0.003);
    END LOOP;
    SELECT max(attempt_no) INTO v_attempt FROM {JOB_SCHEMA}.job_attempts WHERE job_id = {j};
    LOOP
        SELECT status, phase, cancel_requested INTO v_status, v_phase, v_cancel
        FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
        EXIT WHEN v_status <> 'running' OR v_cancel;
        SELECT min(stage_no) INTO v_stage_no
        FROM {JOB_SCHEMA}.job_tasks WHERE job_id = {j} AND state <> 'done';
        IF v_stage_no IS NOT NULL THEN
            SELECT * INTO v_task FROM {JOB_SCHEMA}.job_tasks
            WHERE job_id = {j} AND stage_no = v_stage_no AND state = 'pending'
            ORDER BY task_no LIMIT 1 FOR UPDATE SKIP LOCKED;
        END IF;
        IF v_stage_no IS NULL OR NOT FOUND THEN
            -- Nothing claimable: others hold the last tasks of this stage, or
            -- the controller has not released the next stage yet.
            EXIT WHEN v_stage_no IS NULL AND v_phase = 'validating';
            v_idle := v_idle + 1;
            IF v_idle % 300 = 0 THEN
                PERFORM pg_stat_clear_snapshot();
                EXIT WHEN NOT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE application_name = {sql_literal('pgdm_expand_control:' + job_id)});
            END IF;
            COMMIT;  -- never sit on locks or a snapshot while waiting
            PERFORM pg_sleep(0.003);
            CONTINUE;
        END IF;
        v_started := clock_timestamp();
        v_rows := '{{}}'::jsonb;
        v_seconds := '{{}}'::jsonb;
        v_statements := COALESCE(
            v_task.statements, v_plan -> 'stages' -> (v_stage_no - 1) -> 'statements');
        FOR v_stmt IN SELECT * FROM jsonb_array_elements(v_statements) LOOP
            v_sql := v_stmt ->> 'sql';
            IF v_task.lo IS NOT NULL THEN
                v_sql := replace(replace(v_sql, '{LO_MARK}', v_task.lo::text),
                                 '{HI_MARK}', v_task.hi::text);
            END IF;
            v_t0 := clock_timestamp();
            EXECUTE v_sql;
            GET DIAGNOSTICS v_count = ROW_COUNT;
            IF v_stmt ->> 'key' IS NOT NULL THEN
                v_rows := v_rows || jsonb_build_object(v_stmt ->> 'key', v_count);
                v_seconds := v_seconds || jsonb_build_object(
                    v_stmt ->> 'key', EXTRACT(EPOCH FROM clock_timestamp() - v_t0));
            END IF;
        END LOOP;
        -- Row counters move in the same transaction as the rows, last, so the
        -- counter row locks are held only for the commit itself.
        FOR v_stmt IN SELECT * FROM jsonb_array_elements(v_statements) LOOP
            CONTINUE WHEN NOT COALESCE((v_stmt ->> 'count')::boolean, false);
            UPDATE {ROW_COUNTS_TABLE}
            SET row_count = row_count + (v_rows ->> (v_stmt ->> 'key'))::bigint,
                updated_at = clock_timestamp()
            WHERE schema_name = v_stmt ->> 'schema' AND table_name = v_stmt ->> 'table';
            IF NOT FOUND THEN
                RAISE EXCEPTION 'Contador ausente para %.% durante insercao.',
                    v_stmt ->> 'schema', v_stmt ->> 'table';
            END IF;
        END LOOP;
        UPDATE {JOB_SCHEMA}.job_tasks
        SET state = 'done', rows = v_rows, seconds = v_seconds, xid = txid_current(),
            attempt_no = v_attempt, started_at = v_started, finished_at = clock_timestamp()
        WHERE job_id = {j} AND stage_no = v_task.stage_no AND task_no = v_task.task_no;
        COMMIT;
    END LOOP;
END
$pgdm_worker$;
\\if :ERROR
UPDATE {JOB_SCHEMA}.jobs
SET status = 'failed', error_message = left(:'LAST_ERROR_MESSAGE', 4000),
    error_sqlstate = :'LAST_ERROR_SQLSTATE'
WHERE job_id = {j} AND status = 'running';
\\endif
""".strip() + "\n"


def fail_sql(job_id: str) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    return f"""
\\set pgdm_msg `tail -c 1500 control.err 2>/dev/null`
UPDATE {JOB_SCHEMA}.jobs
SET status = 'failed',
    error_message = COALESCE(NULLIF(btrim(:'pgdm_msg'), ''),
                             'The job controller stopped unexpectedly.')
WHERE job_id = {j} AND status = 'running';
UPDATE {JOB_SCHEMA}.job_attempts a
SET finished_at = clock_timestamp(),
    outcome = (SELECT status FROM {JOB_SCHEMA}.jobs WHERE job_id = {j})
WHERE a.job_id = {j} AND a.finished_at IS NULL;
""".strip() + "\n"


def complete_sql(job_id: str) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    return f"""
\\set ON_ERROR_STOP on
\\set pgdm_versions `tail -n 1 register.out`
BEGIN;
UPDATE {JOB_SCHEMA}.jobs
SET status = 'completed', phase = 'completed',
    registered_versions = :'pgdm_versions'::jsonb, completed_at = clock_timestamp()
WHERE job_id = {j} AND status = 'data_complete';
UPDATE {JOB_SCHEMA}.job_attempts
SET finished_at = clock_timestamp(), outcome = 'completed'
WHERE job_id = {j} AND finished_at IS NULL;
COMMIT;
""".strip() + "\n"


def runner_script(job_id: str, database: str, control_database: str, sql_username: str, workers: int) -> str:
    import shlex

    job_id = checked_job_id(job_id)
    user, db, ctl = shlex.quote(sql_username), shlex.quote(database), shlex.quote(control_database)
    return f"""#!/usr/bin/env bash
# Detached expansion runner: survives the loss of the SSH session that
# started it. All coordination between its psql sessions happens in the
# {JOB_SCHEMA} tables, so killing any of them leaves a resumable job.
cd "$(dirname "$0")" || exit 1
LAUNCH_TS="${{1:-$(date +%s.%N)}}"
P=(psql -h localhost -U {user} -X -qAt -v ON_ERROR_STOP=1)
STATUS_SQL="SELECT status FROM {JOB_SCHEMA}.jobs WHERE job_id = '{job_id}'"
"${{P[@]}}" -d {db} -v launch_ts="$LAUNCH_TS" -f control.sql > control.out 2> control.err &
CONTROL=$!
"${{P[@]}}" -d {db} -f locker.sql > locker.out 2> locker.err &
for i in $(seq 1 {int(workers)}); do
    "${{P[@]}}" -d {db} -f worker.sql > "worker_$i.out" 2> "worker_$i.err" &
done
wait "$CONTROL"
wait
grep -q PGDM_BUSY control.out && exit 0
STATUS=$("${{P[@]}}" -d {db} -c "$STATUS_SQL")
if [ "$STATUS" = data_complete ]; then
    "${{P[@]}}" -d {db} -c "SELECT manifest::text FROM {JOB_SCHEMA}.jobs WHERE job_id = '{job_id}'" > manifest.json \\
        && "${{P[@]}}" -d {ctl} -f register.sql > register.out 2> register.err \\
        && "${{P[@]}}" -d {db} -f complete.sql > complete.out 2> complete.err
    STATUS=$("${{P[@]}}" -d {db} -c "$STATUS_SQL")
fi
if [ "$STATUS" != completed ] && [ "$STATUS" != data_complete ]; then
    "${{P[@]}}" -d {db} -f fail.sql > fail.out 2> fail.err
fi
"""


def launch_script(job_dir: str, files: dict[str, str], job_id: str) -> str:
    marker = f"PGDM_EOF_{checked_job_id(job_id)}"
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", job_dir) or ".." in job_dir:
        raise UnsupportedSql("unsafe remote job directory")
    parts = ["set -e", f'JOBDIR="$HOME"/{job_dir}', 'mkdir -p "$JOBDIR"', 'cd "$JOBDIR"', "chmod 700 ."]
    for name, content in files.items():
        if marker in content:
            raise UnsupportedSql("job file contains the heredoc marker")
        parts.append(f"cat > {name} <<'{marker}'\n{content.rstrip()}\n{marker}")
    parts.append(
        'setsid nohup bash runner.sh "$(date +%s.%N)" > runner.log 2>&1 < /dev/null &'
    )
    parts.append("echo PGDM_LAUNCHED")
    return "\n".join(parts) + "\n"


def probe_sql(source_schema: str, source_table: str, function_names) -> str:
    src = sql_literal(f"{sql_ident(source_schema)}.{sql_ident(source_table)}")
    names = ", ".join(sql_literal(name) for name in sorted(function_names)) or "NULL"
    control_like = sql_literal("pgdm_expand_control:")
    return f"""
\\set ON_ERROR_STOP on
SELECT json_build_object(
    'server_version_num', current_setting('server_version_num')::int,
    'max_connections', current_setting('max_connections')::int,
    'connections', (SELECT count(*) FROM pg_stat_activity),
    'raw_id', (
        SELECT json_build_object('type', format_type(a.atttypid, a.atttypmod),
                                 'notnull', a.attnotnull)
        FROM pg_attribute a
        WHERE a.attrelid = {src}::regclass AND a.attname = 'raw_id'
          AND a.attnum > 0 AND NOT a.attisdropped),
    'aggregates', COALESCE((
        SELECT json_agg(DISTINCT p.proname) FROM pg_proc p
        WHERE p.proname = ANY (ARRAY[{names}]::text[]) AND p.prokind IN ('a', 'w')),
        '[]'::json)
)::text;
SELECT to_regclass('{JOB_SCHEMA}.jobs') IS NOT NULL AS pgdm_has_jobs \\gset
\\if :pgdm_has_jobs
SELECT COALESCE(json_agg(json_build_object(
    'job_id', j.job_id, 'signature', j.signature, 'status', j.status,
    'phase', j.phase, 'error', j.error_message, 'created_at', j.created_at,
    'acknowledged', j.acknowledged,
    'tasks_done', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t
                   WHERE t.job_id = j.job_id AND t.state = 'done'),
    'tasks_total', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t WHERE t.job_id = j.job_id),
    'alive', EXISTS (SELECT 1 FROM pg_stat_activity a
                     WHERE a.application_name = {control_like} || j.job_id)
) ORDER BY j.created_at DESC), '[]'::json)::text
FROM {JOB_SCHEMA}.jobs j
WHERE j.source_schema = {sql_literal(source_schema)}
  AND j.source_table = {sql_literal(source_table)}
  AND (j.status IN ('created', 'running', 'interrupted', 'failed', 'data_complete')
       OR (j.status = 'completed' AND NOT j.acknowledged));
\\else
SELECT '[]';
\\endif
""".strip() + "\n"


def status_sql(job_id: str) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    return (
        "SELECT json_build_object('status', j.status, 'phase', j.phase, "
        "'error', j.error_message, 'sqlstate', j.error_sqlstate, "
        f"'done', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t WHERE t.job_id = j.job_id AND t.state = 'done'), "
        f"'total', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t WHERE t.job_id = j.job_id), "
        "'alive', EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.application_name = "
        f"{sql_literal('pgdm_expand_control:' + job_id)}))::text "
        f"FROM {JOB_SCHEMA}.jobs j WHERE j.job_id = {j};"
    )


def stale_job_sql(job_id: str, destinations) -> str:
    """Whether the rows a job committed are gone: its done tasks inserted rows,
    yet every destination (schema, table) is empty now (TRUNCATE, restore)."""
    job_id = checked_job_id(job_id)
    if not destinations:
        raise ValueError("A stale-job check needs at least one destination table.")
    empty = "".join(
        f"\n    AND NOT EXISTS (SELECT 1 FROM {sql_ident(schema)}.{sql_ident(table)})"
        for schema, table in destinations
    )
    return f"""
SELECT json_build_object('stale', COALESCE((
        SELECT sum(r.value::bigint)
        FROM {JOB_SCHEMA}.job_tasks t, jsonb_each_text(t.rows) r
        WHERE t.job_id = {sql_literal(job_id)} AND t.state = 'done'), 0) > 0{empty}
)::text;
""".strip() + "\n"


def summary_sql(job_id: str, acknowledge: bool) -> str:
    job_id = checked_job_id(job_id)
    j = sql_literal(job_id)
    ack = (
        f"UPDATE {JOB_SCHEMA}.jobs SET acknowledged = true WHERE job_id = {j} AND status = 'completed';\n"
        if acknowledge
        else ""
    )
    return f"""
{ack}SELECT json_build_object(
    'job_id', j.job_id, 'status', j.status, 'error', j.error_message,
    'manifest', j.manifest, 'registered_versions', j.registered_versions,
    'suspended_fks', jsonb_array_length(j.suspended_fks),
    'suspended_indexes', jsonb_array_length(j.suspended_indexes), 'config', j.config,
    'plan_stages', (SELECT json_agg(json_build_object(
            'stage_no', (s ->> 'stage_no')::int, 'kind', s ->> 'kind', 'label', s ->> 'label'))
        FROM jsonb_array_elements(j.plan -> 'stages') s),
    'attempts', (SELECT json_agg(json_build_object(
            'attempt_no', a.attempt_no, 'workers', a.workers, 'outcome', a.outcome,
            'seconds', EXTRACT(EPOCH FROM COALESCE(a.finished_at, clock_timestamp()) - a.started_at),
            'tasks', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t
                      WHERE t.job_id = a.job_id AND t.attempt_no = a.attempt_no))
            ORDER BY a.attempt_no)
        FROM {JOB_SCHEMA}.job_attempts a WHERE a.job_id = j.job_id),
    'stages', (SELECT json_agg(json_build_object(
            'stage_no', s.stage_no, 'tasks', s.tasks, 'seconds', s.seconds) ORDER BY s.stage_no)
        FROM (SELECT t.stage_no, count(*) AS tasks,
                     EXTRACT(EPOCH FROM max(t.finished_at) - min(t.started_at)) AS seconds
              FROM {JOB_SCHEMA}.job_tasks t WHERE t.job_id = j.job_id GROUP BY t.stage_no) s),
    'statements', (SELECT json_agg(json_build_object(
            'key', q.key, 'rows', q.rows, 'seconds', q.seconds, 'tasks', q.tasks) ORDER BY q.first_stage, q.key)
        FROM (SELECT r.key, sum(r.value::bigint) AS rows, count(*) AS tasks,
                     min(t.stage_no) AS first_stage,
                     sum((t.seconds ->> r.key)::double precision) AS seconds
              FROM {JOB_SCHEMA}.job_tasks t, jsonb_each_text(t.rows) r
              WHERE t.job_id = j.job_id GROUP BY r.key) q)
)::text
FROM {JOB_SCHEMA}.jobs j WHERE j.job_id = {j};
""".strip() + "\n"


def cancel_sql(job_id: str) -> str:
    job_id = checked_job_id(job_id)
    return (
        f"UPDATE {JOB_SCHEMA}.jobs SET cancel_requested = true "
        f"WHERE job_id = {sql_literal(job_id)} AND status IN ('created', 'running');"
    )


def abort_sql(job_id: str) -> str:
    job_id = checked_job_id(job_id)
    """Undo an unfinished job: remove exactly the rows its committed tasks
    inserted, put the row counters and foreign keys back, drop its artifacts."""
    j = sql_literal(job_id)
    apps = ", ".join(
        sql_literal(f"pgdm_expand_{role}:{job_id}") for role in ("control", "locker", "worker")
    )
    return f"""
\\set ON_ERROR_STOP on
SET client_min_messages = warning;
{OWNERSHIP_GUARD}
-- Stop every session of the job before touching its rows.
UPDATE {JOB_SCHEMA}.jobs SET cancel_requested = true
WHERE job_id = {j} AND status IN ('created', 'running', 'interrupted', 'failed');
DO $pgdm_abort$
DECLARE
    v_started timestamptz := clock_timestamp();
BEGIN
    LOOP
        PERFORM pg_stat_clear_snapshot();
        PERFORM pg_cancel_backend(pid) FROM pg_stat_activity
        WHERE application_name IN ({apps}) AND pid <> pg_backend_pid()
          AND clock_timestamp() - v_started > interval '2 seconds';
        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM pg_stat_activity WHERE application_name IN ({apps}));
        IF clock_timestamp() - v_started > interval '120 seconds' THEN
            RAISE EXCEPTION 'The sessions of expansion job % did not stop.', {j};
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
END
$pgdm_abort$;
DO $pgdm_abort$
DECLARE
    j {JOB_SCHEMA}.jobs%ROWTYPE;
    v_dest jsonb;
    v_fk jsonb;
    v_sql text;
    v_tables text;
    v_removed bigint;
    v_total bigint := 0;
BEGIN
    SELECT * INTO j FROM {JOB_SCHEMA}.jobs WHERE job_id = {j} FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Expansion job % was not found.', {j};
    END IF;
    IF j.status NOT IN ('created', 'running', 'interrupted', 'failed') THEN
        RAISE EXCEPTION 'Expansion job % is % and can no longer be discarded.', j.job_id, j.status;
    END IF;
    IF NOT pg_try_advisory_xact_lock({_lock_key(job_id)}) THEN
        RAISE EXCEPTION 'Expansion job % is still running on the server.', j.job_id;
    END IF;
    SELECT string_agg(format('%I.%I', d ->> 'schema', d ->> 'table'), ', ') INTO v_tables
    FROM jsonb_array_elements(j.plan -> 'destinations') d;
    EXECUTE 'LOCK TABLE ' || v_tables || ' IN ACCESS EXCLUSIVE MODE';
    -- Children first: later destinations reference earlier ones.
    FOR v_dest IN
        SELECT d.value FROM jsonb_array_elements(j.plan -> 'destinations')
            WITH ORDINALITY AS d(value, n) ORDER BY d.n DESC
    LOOP
        EXECUTE format(
            'DELETE FROM %I.%I AS d USING {JOB_SCHEMA}.job_tasks AS t '
            'WHERE t.job_id = %L AND t.state = ''done'' AND t.xid IS NOT NULL '
            'AND d.xmin::text::bigint = (t.xid & 4294967295)',
            v_dest ->> 'schema', v_dest ->> 'table', j.job_id);
        GET DIAGNOSTICS v_removed = ROW_COUNT;
        v_total := v_total + v_removed;
        UPDATE {ROW_COUNTS_TABLE}
        SET row_count = GREATEST(row_count - v_removed, 0), updated_at = clock_timestamp()
        WHERE schema_name = v_dest ->> 'schema' AND table_name = v_dest ->> 'table';
    END LOOP;
    FOR v_fk IN SELECT * FROM jsonb_array_elements(j.suspended_indexes) LOOP
        IF to_regclass(format('%I.%I', v_fk ->> 'schema', v_fk ->> 'index')) IS NULL THEN
            EXECUTE v_fk ->> 'definition';
        END IF;
        IF v_fk ->> 'constraint' IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = v_fk ->> 'schema' AND r.relname = v_fk ->> 'table'
              AND c.conname = v_fk ->> 'constraint')
        THEN
            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s USING INDEX %I',
                           v_fk ->> 'schema', v_fk ->> 'table', v_fk ->> 'constraint',
                           CASE v_fk ->> 'contype' WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                           v_fk ->> 'index');
            IF v_fk ->> 'constraint_comment' IS NOT NULL THEN
                EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
                               v_fk ->> 'constraint', v_fk ->> 'schema', v_fk ->> 'table',
                               v_fk ->> 'constraint_comment');
            END IF;
        END IF;
        IF v_fk ->> 'index_comment' IS NOT NULL THEN
            EXECUTE format('COMMENT ON INDEX %I.%I IS %L', v_fk ->> 'schema',
                           v_fk ->> 'index', v_fk ->> 'index_comment');
        END IF;
    END LOOP;
    FOR v_fk IN SELECT * FROM jsonb_array_elements(j.suspended_fks) LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = v_fk ->> 'schema' AND r.relname = v_fk ->> 'table'
              AND c.conname = v_fk ->> 'name')
        THEN
            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                           v_fk ->> 'schema', v_fk ->> 'table', v_fk ->> 'name',
                           v_fk ->> 'definition');
            IF v_fk ->> 'comment' IS NOT NULL THEN
                EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
                               v_fk ->> 'name', v_fk ->> 'schema', v_fk ->> 'table',
                               v_fk ->> 'comment');
            END IF;
        ELSE
            EXECUTE format('ALTER TABLE %I.%I VALIDATE CONSTRAINT %I',
                           v_fk ->> 'schema', v_fk ->> 'table', v_fk ->> 'name');
        END IF;
    END LOOP;
    FOR v_fk IN SELECT * FROM jsonb_array_elements(j.sequence_cache) LOOP
        EXECUTE format('ALTER SEQUENCE %s CACHE %s', v_fk ->> 'name', v_fk ->> 'cache');
    END LOOP;
    FOR v_sql IN SELECT jsonb_array_elements_text(j.plan -> 'cleanup') LOOP
        EXECUTE v_sql;
    END LOOP;
    UPDATE {JOB_SCHEMA}.job_attempts
    SET finished_at = COALESCE(finished_at, clock_timestamp()),
        outcome = COALESCE(outcome, 'aborted')
    WHERE job_id = j.job_id AND finished_at IS NULL;
    UPDATE {JOB_SCHEMA}.jobs
    SET status = 'aborted', phase = 'aborted', completed_at = clock_timestamp(),
        error_message = format('Discarded: %s inserted rows were removed.', v_total)
    WHERE job_id = j.job_id;
END
$pgdm_abort$;
SELECT json_build_object('job_id', job_id, 'status', status, 'detail', error_message)::text
FROM {JOB_SCHEMA}.jobs WHERE job_id = {j};
""".strip() + "\n"


def request_sql(source_schema: str, source_table: str) -> str:
    # The stored request is offered back to the user for "Resume": only trust
    # it from job tables nobody else can write to. (It still goes through the
    # complete recipe validation and must reproduce the job's signature.)
    return f"""
\\set ON_ERROR_STOP on
SET client_min_messages = warning;
{OWNERSHIP_GUARD}
SELECT to_regclass('{JOB_SCHEMA}.jobs') IS NOT NULL AS pgdm_has_jobs \\gset
\\if :pgdm_has_jobs
SELECT json_build_object(
    'job_id', j.job_id, 'status', j.status, 'request', j.request,
    'error', j.error_message, 'created_at', j.created_at,
    'tasks_done', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t
                   WHERE t.job_id = j.job_id AND t.state = 'done'),
    'tasks_total', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t WHERE t.job_id = j.job_id)
)::text
FROM {JOB_SCHEMA}.jobs j
WHERE j.source_schema = {sql_literal(source_schema)}
  AND j.source_table = {sql_literal(source_table)}
  AND j.status IN ('created', 'running', 'interrupted', 'failed', 'data_complete');
\\endif
""".strip() + "\n"


def list_jobs_sql() -> str:
    return f"""
SELECT to_regclass('{JOB_SCHEMA}.jobs') IS NOT NULL AS pgdm_has_jobs \\gset
\\if :pgdm_has_jobs
SELECT COALESCE(json_agg(json_build_object(
    'job_id', j.job_id, 'status', j.status, 'phase', j.phase,
    'source', j.source_schema || '.' || j.source_table,
    'created_at', j.created_at, 'completed_at', j.completed_at,
    'error', j.error_message, 'workers', j.config ->> 'workers',
    'suspended_foreign_keys', jsonb_array_length(j.suspended_fks),
    'suspended_indexes', jsonb_array_length(j.suspended_indexes),
    'tasks_done', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t
                   WHERE t.job_id = j.job_id AND t.state = 'done'),
    'tasks_total', (SELECT count(*) FROM {JOB_SCHEMA}.job_tasks t WHERE t.job_id = j.job_id),
    'attempts', (SELECT count(*) FROM {JOB_SCHEMA}.job_attempts a WHERE a.job_id = j.job_id),
    'registered_versions', j.registered_versions
) ORDER BY j.created_at DESC), '[]'::json)::text
FROM {JOB_SCHEMA}.jobs j;
\\else
SELECT '[]';
\\endif
""".strip() + "\n"
