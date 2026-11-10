import re
import unittest
from pathlib import Path

from services import expansion_engine as engine

JOB = "0123456789abcdef0123456789abcdef"
RECIPES = Path(__file__).resolve().parent.parent / "SQL files"


def destination(table, sql, reads=(), functions=()):
    statement = engine.analyze_statement(sql)
    guarded = sql.replace(
        engine.PLACEHOLDER, engine.source_sql("public", "raw_data", ["group001"], False)
    )
    return engine.Destination("public", table, [statement], [guarded], set(reads), set(functions))


def official_destinations():
    result = []
    for path in sorted(RECIPES.glob("*.sql")):
        table = re.fullmatch(r"\d+_(.+)\.sql", path.name).group(1)
        sql = path.read_text(encoding="utf-8")
        reads = {
            ("public", name)
            for name in re.findall(r"public\.(table\d)", sql.split("SELECT", 1)[1])
            if name != table
        }
        result.append(destination(table, sql, reads))
    return result


def plan(destinations, aggregates=(), chunkable=True):
    return engine.build_plan(
        JOB, "public", "raw_data", ["group001"], destinations, set(aggregates), chunkable
    )


class OfficialWorkloadPlanTests(unittest.TestCase):
    def test_official_recipes_become_two_parallel_passes(self):
        stages = plan(official_destinations())["stages"]
        self.assertEqual([s["kind"] for s in stages], ["chunks", "single", "chunks"])
        self.assertEqual(
            [s["key"] for s in stages[2]["statements"]],
            ["public.table1|1", "public.table5|1", "public.table6|1", "public.table2|1"],
        )
        self.assertEqual(
            [s["key"] for s in stages[0]["statements"]],
            ["partial|public.table3|1", "partial|public.table4|1"],
        )

    def test_chunk_statements_only_restrict_the_source_range(self):
        result = plan(official_destinations())
        for statement in result["stages"][2]["statements"]:
            self.assertIn(engine.LO_MARK, statement["sql"])
            self.assertIn('"raw_schema" IN (\'group001\')', statement["sql"])
            self.assertNotIn(engine.PLACEHOLDER, statement["sql"])
        # the unique index the chunk joins rely on is kept during the load
        self.assertEqual(result["keep_index_columns"], {"public.table1": ["id_column_3"]})

    def test_recipe_files_are_the_official_ones(self):
        # the engine rewrites execution, never the versioned recipe text
        for path in RECIPES.glob("*.sql"):
            self.assertIn(engine.PLACEHOLDER, path.read_text(encoding="utf-8"))

    def test_source_without_usable_raw_id_runs_whole_source(self):
        stages = plan(official_destinations(), chunkable=False)["stages"]
        self.assertTrue(all(stage["kind"] == "single" for stage in stages))
        self.assertEqual(len(stages), 6)


class ChunkSafetyTests(unittest.TestCase):
    BASE = "INSERT INTO public.t1 (a, b) SELECT s.raw_id, s.raw ->> 'x' FROM {{source}} AS s"

    def kinds(self, *destinations, aggregates=()):
        return [stage["kind"] for stage in plan(list(destinations), aggregates)["stages"]]

    def test_plain_rowwise_statement_is_chunked(self):
        self.assertEqual(self.kinds(destination("t1", self.BASE)), ["chunks"])

    def test_cross_row_constructs_run_whole_source(self):
        for tail in (
            " GROUP BY 1, 2",
            " ORDER BY 1",
            " LIMIT 10",
            " UNION ALL SELECT 1, 'x'",
            " WHERE s.raw_id IN (SELECT 1)",
        ):
            with self.subTest(tail=tail):
                self.assertEqual(self.kinds(destination("t1", self.BASE + tail)), ["single"])
        windowed = self.BASE.replace("s.raw ->> 'x'", "(row_number() OVER ())::text")
        self.assertEqual(self.kinds(destination("t1", windowed)), ["single"])

    def test_aggregate_function_runs_whole_source(self):
        sql = "INSERT INTO public.t1 (a) SELECT my_agg(s.raw_id) FROM {{source}} AS s"
        dest = destination("t1", sql, functions={"my_agg"})
        self.assertEqual(self.kinds(dest, aggregates={"my_agg"}), ["single"])

    def test_right_join_runs_whole_source_but_right_function_is_fine(self):
        joined = self.BASE + " RIGHT JOIN public.t0 AS d ON d.k = s.raw_id"
        self.assertEqual(self.kinds(destination("t1", joined, {("public", "t0")})), ["single"])
        function = self.BASE.replace("s.raw ->> 'x'", "RIGHT(s.raw ->> 'x', 1)")
        self.assertEqual(self.kinds(destination("t1", function)), ["chunks"])

    def test_source_must_be_first_and_only_once(self):
        twice = self.BASE + " JOIN {{source}} AS o ON o.raw_id = s.raw_id"
        self.assertEqual(self.kinds(destination("t1", twice)), ["single"])

    def test_reader_shares_a_chunk_only_through_a_raw_id_key(self):
        parent = destination("t1", self.BASE)
        keyed = destination(
            "t2",
            "INSERT INTO public.t2 (p) SELECT m.a FROM {{source}} AS s "
            "JOIN public.t1 AS m ON m.a = s.raw_id",
            {("public", "t1")},
        )
        self.assertEqual(self.kinds(parent, keyed), ["chunks"])
        bounded = plan([parent, keyed])["stages"][0]["statements"][1]["sql"]
        self.assertIn(f"m.a >= {engine.LO_MARK} AND m.a <= {engine.HI_MARK}", bounded)

        # joined on something that is not the source raw_id: the parent must be
        # complete first, so the reader gets its own later pass
        loose = destination(
            "t2",
            "INSERT INTO public.t2 (p) SELECT m.a FROM {{source}} AS s "
            "JOIN public.t1 AS m ON m.b = s.raw ->> 'x'",
            {("public", "t1")},
        )
        self.assertEqual(self.kinds(parent, loose), ["chunks", "chunks"])

        # keyed on a column the parent does not fill from the raw id
        other_column = destination(
            "t2",
            "INSERT INTO public.t2 (p) SELECT m.a FROM {{source}} AS s "
            "JOIN public.t1 AS m ON m.b = s.raw_id",
            {("public", "t1")},
        )
        self.assertEqual(self.kinds(parent, other_column), ["chunks", "chunks"])

        # an OR next to the key no longer guarantees chunk locality
        with_or = destination(
            "t2",
            "INSERT INTO public.t2 (p) SELECT m.a FROM {{source}} AS s "
            "JOIN public.t1 AS m ON m.a = s.raw_id OR m.b = 'z'",
            {("public", "t1")},
        )
        self.assertEqual(self.kinds(parent, with_or), ["chunks", "chunks"])

    def test_statement_reading_its_own_destination_runs_whole_source(self):
        own = destination(
            "t1",
            self.BASE + " LEFT JOIN public.t1 AS old ON old.a = s.raw_id",
            {("public", "t1")},
        )
        self.assertEqual(self.kinds(own), ["single"])

    def test_distinct_is_split_into_partial_and_merge(self):
        sql = "INSERT INTO public.d (v) SELECT DISTINCT s.raw ->> 'x' FROM {{source}} AS s"
        result = plan([destination("d", sql)])
        self.assertEqual([s["kind"] for s in result["stages"]], ["chunks", "single"])
        self.assertIn("SELECT DISTINCT c1 FROM", result["stages"][1]["statements"][0]["sql"])
        self.assertTrue(result["setup"][0].rstrip().endswith("WITH NO DATA"))
        nested = sql.replace("s.raw ->> 'x'", "(s.raw ->> 'x') IS DISTINCT FROM 'a'")
        self.assertEqual([s["kind"] for s in plan([destination("d", nested)])["stages"]], ["single"])

    def test_reserved_marker_in_a_recipe_is_refused(self):
        sql = self.BASE.replace("'x'", f"'{engine.LO_MARK}'")
        with self.assertRaises(engine.UnsupportedSql):
            plan([destination("t1", sql)])


class RewriteTests(unittest.TestCase):
    def test_lateral_values_becomes_union_all_in_the_same_order(self):
        sql = (
            "INSERT INTO public.t (a, b) SELECT v.n, v.t FROM {{source}} AS s "
            "CROSS JOIN LATERAL (VALUES (1::smallint, s.raw ->> 'a'), (2::smallint, f(s.raw, ')'))) "
            "AS v(n, t) WHERE v.n > 0"
        )
        rewritten = engine.unroll_lateral_values(sql)
        self.assertNotIn("VALUES", rewritten)
        self.assertLess(rewritten.index("SELECT 1::smallint"), rewritten.index("SELECT 2::smallint"))
        self.assertIn("UNION ALL SELECT 2::smallint, f(s.raw, ')')", rewritten)
        self.assertTrue(rewritten.endswith("AS v(n, t) WHERE v.n > 0"))

    def test_values_without_column_aliases_is_left_alone(self):
        sql = (
            "INSERT INTO public.t (a) SELECT v.column1 FROM {{source}} AS s "
            "CROSS JOIN LATERAL (VALUES (1), (2)) AS v"
        )
        self.assertEqual(engine.unroll_lateral_values(sql), sql)

    def test_lexer_ignores_keywords_inside_literals_and_comments(self):
        sql = (
            "INSERT INTO public.t (a) SELECT 'GROUP BY ''x''' /* ORDER BY */ -- LIMIT\n"
            "FROM {{source}} AS s"
        )
        self.assertTrue(engine.analyze_statement(sql).rowwise)

    def test_signature_depends_on_recipes_and_scope(self):
        one = [destination("t1", ChunkSafetyTests.BASE)]
        two = [destination("t1", ChunkSafetyTests.BASE + " WHERE s.raw_id > 5")]
        base = engine.job_signature("db", "public", "raw_data", ["g1"], one)
        self.assertEqual(base, engine.job_signature("db", "public", "raw_data", ["g1"], one))
        self.assertNotEqual(base, engine.job_signature("db", "public", "raw_data", ["g1"], two))
        self.assertNotEqual(base, engine.job_signature("db", "public", "raw_data", ["g2"], one))


class GeneratedSqlTests(unittest.TestCase):
    def test_server_scripts_render(self):
        config = {"workers": 4, "sequence_cache": 256}
        control = engine.control_sql(JOB, "public", "raw_data", ["group001"], config, True)
        self.assertIn("pg_try_advisory_lock", control)
        self.assertIn("NOT VALID", control)
        self.assertIn("USING INDEX", control)
        for text in (
            control,
            engine.worker_sql(JOB),
            engine.locker_sql(JOB, "public", "raw_data"),
            engine.abort_sql(JOB),
        ):
            # liveness checks must never read a stale pg_stat_activity snapshot
            self.assertIn("pg_stat_clear_snapshot()", text)
        script = engine.launch_script("store/expansion_jobs/" + JOB, {"a.sql": "SELECT 1;"}, JOB)
        self.assertIn("setsid nohup bash runner.sh", script)

    def test_stale_job_check_names_every_destination(self):
        sql = engine.stale_job_sql(JOB, [("public", "t1"), ("public", "t2")])
        self.assertIn(f"t.job_id = '{JOB}'", sql)
        self.assertIn("jsonb_each_text(t.rows)", sql)
        for table in ("t1", "t2"):
            self.assertIn(f"NOT EXISTS (SELECT 1 FROM {engine.sql_ident('public')}."
                          f"{engine.sql_ident(table)})", sql)
        with self.assertRaises(engine.UnsupportedSql):
            engine.stale_job_sql("x'; DROP TABLE t1; --", [("public", "t1")])


if __name__ == "__main__":
    unittest.main()
