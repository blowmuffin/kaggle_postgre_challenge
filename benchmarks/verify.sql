-- Compare the six destinations with the reference snapshot (snapshot_reference.sql).
--
-- Surrogate identity values (id_column_1) are assigned by sequences, so their
-- numbering depends on insert order (in the original engine too: its order
-- follows the physical scan). Rows are therefore compared through their
-- natural keys: table1 by the Raw id it came from, table3 by its value tuple,
-- and every child row by its parent's natural key. Every other column,
-- including every foreign-key relationship, must match exactly.
-- Each line must report differences = 0 / ok = t.
\pset format unaligned
\pset tuples_only on
\pset fieldsep ' | '

CREATE TEMP VIEW v_cur_t1 AS
SELECT t.id_column_1 AS surrogate, t.id_column_2, t.text_column_1, t.datetime_column_1,
       t.text_column_2, t.integer_column_1, t.integer_column_2, t.integer_column_3,
       t.text_column_3, t.id_column_3, t.id_column_5, t.integer_column_4,
       t.integer_column_5, t.id_column_6, t.integer_column_6, t.integer_column_7,
       d.text_column_1 AS d1, d.text_column_2 AS d2, d.numeric_column_1 AS d3,
       d.text_column_3 AS d4, d.text_column_4 AS d5
FROM public.table1 t LEFT JOIN public.table3 d ON d.id_column_1 = t.id_column_4;
CREATE TEMP VIEW v_ref_t1 AS
SELECT t.id_column_1 AS surrogate, t.id_column_2, t.text_column_1, t.datetime_column_1,
       t.text_column_2, t.integer_column_1, t.integer_column_2, t.integer_column_3,
       t.text_column_3, t.id_column_3, t.id_column_5, t.integer_column_4,
       t.integer_column_5, t.id_column_6, t.integer_column_6, t.integer_column_7,
       d.text_column_1 AS d1, d.text_column_2 AS d2, d.numeric_column_1 AS d3,
       d.text_column_3 AS d4, d.text_column_4 AS d5
FROM expand_reference.table1 t LEFT JOIN expand_reference.table3 d ON d.id_column_1 = t.id_column_4;

CREATE TEMP TABLE verify_result (check_name text, differences bigint);

INSERT INTO verify_result
SELECT 'table3 rows (natural tuple)', count(*) FROM (
    (SELECT text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4 FROM public.table3
     EXCEPT ALL
     SELECT text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4 FROM expand_reference.table3)
    UNION ALL
    (SELECT text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4 FROM expand_reference.table3
     EXCEPT ALL
     SELECT text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4 FROM public.table3)) x;

INSERT INTO verify_result
SELECT 'table4 rows (all columns)', count(*) FROM (
    (SELECT * FROM public.table4 EXCEPT ALL SELECT * FROM expand_reference.table4)
    UNION ALL
    (SELECT * FROM expand_reference.table4 EXCEPT ALL SELECT * FROM public.table4)) x;

INSERT INTO verify_result
SELECT 'table1 rows (by raw id, table3 by value)', count(*) FROM (
    (SELECT id_column_2, text_column_1, datetime_column_1, text_column_2, integer_column_1, integer_column_2, integer_column_3, text_column_3, id_column_3, id_column_5, integer_column_4, integer_column_5, id_column_6, integer_column_6, integer_column_7, d1, d2, d3, d4, d5 FROM v_cur_t1
     EXCEPT ALL
     SELECT id_column_2, text_column_1, datetime_column_1, text_column_2, integer_column_1, integer_column_2, integer_column_3, text_column_3, id_column_3, id_column_5, integer_column_4, integer_column_5, id_column_6, integer_column_6, integer_column_7, d1, d2, d3, d4, d5 FROM v_ref_t1)
    UNION ALL
    (SELECT id_column_2, text_column_1, datetime_column_1, text_column_2, integer_column_1, integer_column_2, integer_column_3, text_column_3, id_column_3, id_column_5, integer_column_4, integer_column_5, id_column_6, integer_column_6, integer_column_7, d1, d2, d3, d4, d5 FROM v_ref_t1
     EXCEPT ALL
     SELECT id_column_2, text_column_1, datetime_column_1, text_column_2, integer_column_1, integer_column_2, integer_column_3, text_column_3, id_column_3, id_column_5, integer_column_4, integer_column_5, id_column_6, integer_column_6, integer_column_7, d1, d2, d3, d4, d5 FROM v_cur_t1)) x;

INSERT INTO verify_result
SELECT 'table5 rows (parent by raw id)', count(*) FROM (
    (SELECT p.id_column_3 AS raw_id, c.numeric_column_1, c.numeric_column_2, c.integer_column_1, c.integer_column_2, c.id_column_3 FROM public.table5 c LEFT JOIN public.table1 p ON p.id_column_1 = c.id_column_2
     EXCEPT ALL
     SELECT p.id_column_3, c.numeric_column_1, c.numeric_column_2, c.integer_column_1, c.integer_column_2, c.id_column_3 FROM expand_reference.table5 c LEFT JOIN expand_reference.table1 p ON p.id_column_1 = c.id_column_2)
    UNION ALL
    (SELECT p.id_column_3, c.numeric_column_1, c.numeric_column_2, c.integer_column_1, c.integer_column_2, c.id_column_3 FROM expand_reference.table5 c LEFT JOIN expand_reference.table1 p ON p.id_column_1 = c.id_column_2
     EXCEPT ALL
     SELECT p.id_column_3, c.numeric_column_1, c.numeric_column_2, c.integer_column_1, c.integer_column_2, c.id_column_3 FROM public.table5 c LEFT JOIN public.table1 p ON p.id_column_1 = c.id_column_2)) x;

INSERT INTO verify_result
SELECT 'table6 rows (parent by raw id)', count(*) FROM (
    (SELECT p.id_column_3 AS raw_id, c.id_column_3 FROM public.table6 c LEFT JOIN public.table1 p ON p.id_column_1 = c.id_column_2
     EXCEPT ALL
     SELECT p.id_column_3, c.id_column_3 FROM expand_reference.table6 c LEFT JOIN expand_reference.table1 p ON p.id_column_1 = c.id_column_2)
    UNION ALL
    (SELECT p.id_column_3, c.id_column_3 FROM expand_reference.table6 c LEFT JOIN expand_reference.table1 p ON p.id_column_1 = c.id_column_2
     EXCEPT ALL
     SELECT p.id_column_3, c.id_column_3 FROM public.table6 c LEFT JOIN public.table1 p ON p.id_column_1 = c.id_column_2)) x;

INSERT INTO verify_result
SELECT 'table2 rows (parent by raw id)', count(*) FROM (
    (SELECT p.id_column_3 AS raw_id, c.integer_column_1, c.integer_column_2, c.text_column_1, c.integer_column_3, c.integer_column_4, c.integer_column_5, c.integer_column_6 FROM public.table2 c LEFT JOIN public.table1 p ON p.id_column_1 = c.id_column_2
     EXCEPT ALL
     SELECT p.id_column_3, c.integer_column_1, c.integer_column_2, c.text_column_1, c.integer_column_3, c.integer_column_4, c.integer_column_5, c.integer_column_6 FROM expand_reference.table2 c LEFT JOIN expand_reference.table1 p ON p.id_column_1 = c.id_column_2)
    UNION ALL
    (SELECT p.id_column_3, c.integer_column_1, c.integer_column_2, c.text_column_1, c.integer_column_3, c.integer_column_4, c.integer_column_5, c.integer_column_6 FROM expand_reference.table2 c LEFT JOIN expand_reference.table1 p ON p.id_column_1 = c.id_column_2
     EXCEPT ALL
     SELECT p.id_column_3, c.integer_column_1, c.integer_column_2, c.text_column_1, c.integer_column_3, c.integer_column_4, c.integer_column_5, c.integer_column_6 FROM public.table2 c LEFT JOIN public.table1 p ON p.id_column_1 = c.id_column_2)) x;

-- Surrogate keys must still be unique and every foreign key must resolve.
INSERT INTO verify_result
SELECT 'duplicate surrogate keys', (SELECT count(*) - count(DISTINCT id_column_1) FROM public.table1)
     + (SELECT count(*) - count(DISTINCT id_column_1) FROM public.table2)
     + (SELECT count(*) - count(DISTINCT id_column_1) FROM public.table5)
     + (SELECT count(*) - count(DISTINCT id_column_1) FROM public.table6);

-- Schema must be exactly as before: constraints (all validated), indexes, sequences.
INSERT INTO verify_result
SELECT 'constraint definitions', count(*) FROM (
    (SELECT conrelid::regclass::text, conname, contype::text, convalidated, pg_get_constraintdef(oid)
     FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text ~ '^table[1-6]$'
     EXCEPT ALL SELECT * FROM expand_reference.constraints)
    UNION ALL
    (SELECT * FROM expand_reference.constraints
     EXCEPT ALL
     SELECT conrelid::regclass::text, conname, contype::text, convalidated, pg_get_constraintdef(oid)
     FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text ~ '^table[1-6]$')) x;

INSERT INTO verify_result
SELECT 'index definitions', count(*) FROM (
    (SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename ~ '^table[1-6]$'
     EXCEPT ALL SELECT * FROM expand_reference.indexes)
    UNION ALL
    (SELECT * FROM expand_reference.indexes
     EXCEPT ALL
     SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename ~ '^table[1-6]$')) x;

INSERT INTO verify_result
SELECT 'sequence settings', count(*) FROM (
    SELECT seqrelid::regclass::text, seqcache, seqincrement FROM pg_sequence WHERE seqrelid::regclass::text ~ '^table[1-6]_'
    EXCEPT ALL SELECT * FROM expand_reference.sequences) x;

INSERT INTO verify_result
SELECT 'invalid indexes', count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
WHERE c.relnamespace = 'public'::regnamespace AND c.relname ~ '^table[1-6]$' AND NOT (i.indisvalid AND i.indisready);

-- Row counters maintained by the Manager must equal the real row counts.
INSERT INTO verify_result
SELECT 'row counter drift', sum(abs(c.row_count - r.n)) FROM public.pgdm_table_row_counts c
JOIN (SELECT 'table1' AS t, count(*) AS n FROM public.table1 UNION ALL SELECT 'table2', count(*) FROM public.table2
      UNION ALL SELECT 'table3', count(*) FROM public.table3 UNION ALL SELECT 'table4', count(*) FROM public.table4
      UNION ALL SELECT 'table5', count(*) FROM public.table5 UNION ALL SELECT 'table6', count(*) FROM public.table6) r
  ON c.schema_name = 'public' AND c.table_name = r.t;

INSERT INTO verify_result
SELECT 'leftover job artifacts (partial tables)', count(*) FROM pg_tables
WHERE schemaname = 'pgdm_expansion' AND tablename LIKE 'p\_%';

SELECT check_name, 'differences = ' || differences, CASE WHEN differences = 0 THEN 'ok' ELSE 'FAIL' END
FROM verify_result;
SELECT 'rows: ' || string_agg(t || '=' || n, ', ' ORDER BY t) FROM (
    SELECT 'table1' AS t, count(*) AS n FROM public.table1 UNION ALL SELECT 'table2', count(*) FROM public.table2
    UNION ALL SELECT 'table3', count(*) FROM public.table3 UNION ALL SELECT 'table4', count(*) FROM public.table4
    UNION ALL SELECT 'table5', count(*) FROM public.table5 UNION ALL SELECT 'table6', count(*) FROM public.table6) r;
SELECT CASE WHEN sum(differences) = 0 THEN 'VERIFY PASS' ELSE 'VERIFY FAIL' END FROM verify_result;
