-- Benchmark helper: put back any index / constraint / sequence setting of the
-- six destinations that differs from the reference snapshot. Only needed when
-- a benchmark reset wiped an *unfinished* job's bookkeeping (reset.sql drops
-- the job schema); normal operation restores these through Resume or Discard.
DO $$
DECLARE
    r record;
BEGIN
    IF to_regclass('expand_reference.indexes') IS NULL THEN
        RETURN;
    END IF;
    FOR r IN SELECT * FROM expand_reference.indexes i
             WHERE to_regclass('public.' || quote_ident(i.indexname)) IS NULL ORDER BY indexname LOOP
        EXECUTE r.indexdef;
    END LOOP;
    FOR r IN SELECT * FROM expand_reference.constraints c
             WHERE c.contype IN ('p', 'u') AND NOT EXISTS (
                 SELECT 1 FROM pg_constraint x
                 WHERE x.conrelid = ('public.' || c.table_name)::regclass AND x.conname = c.conname)
             ORDER BY conname LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s USING INDEX %I', r.table_name,
                       r.conname, CASE r.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, r.conname);
    END LOOP;
    FOR r IN SELECT * FROM expand_reference.constraints c
             WHERE c.contype = 'f' AND NOT EXISTS (
                 SELECT 1 FROM pg_constraint x
                 WHERE x.conrelid = ('public.' || c.table_name)::regclass AND x.conname = c.conname)
             ORDER BY conname LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s', r.table_name, r.conname, r.definition);
    END LOOP;
    FOR r IN SELECT c.conrelid::regclass AS tbl, c.conname FROM pg_constraint c
             WHERE c.connamespace = 'public'::regnamespace AND NOT c.convalidated LOOP
        EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', r.tbl, r.conname);
    END LOOP;
    FOR r IN SELECT * FROM expand_reference.sequences LOOP
        EXECUTE format('ALTER SEQUENCE public.%I CACHE %s', r.sequence_name, r.seqcache);
    END LOOP;
END $$;
