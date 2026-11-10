-- Keep a copy of the six destinations as produced by a reference run
-- (PDM_EXPAND_ENGINE=legacy), to compare a later run against with verify.sql.
DROP SCHEMA IF EXISTS expand_reference CASCADE;
CREATE SCHEMA expand_reference;
CREATE TABLE expand_reference.table1 AS SELECT * FROM public.table1;
CREATE TABLE expand_reference.table2 AS SELECT * FROM public.table2;
CREATE TABLE expand_reference.table3 AS SELECT * FROM public.table3;
CREATE TABLE expand_reference.table4 AS SELECT * FROM public.table4;
CREATE TABLE expand_reference.table5 AS SELECT * FROM public.table5;
CREATE TABLE expand_reference.table6 AS SELECT * FROM public.table6;
CREATE TABLE expand_reference.constraints AS
SELECT conrelid::regclass::text AS table_name, conname, contype::text AS contype,
       convalidated, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text ~ '^table[1-6]$';
CREATE TABLE expand_reference.indexes AS
SELECT tablename, indexname, indexdef FROM pg_indexes
WHERE schemaname = 'public' AND tablename ~ '^table[1-6]$';
CREATE TABLE expand_reference.sequences AS
SELECT seqrelid::regclass::text AS sequence_name, seqcache, seqincrement
FROM pg_sequence WHERE seqrelid::regclass::text ~ '^table[1-6]_';
SELECT 'reference snapshot stored in schema expand_reference';
