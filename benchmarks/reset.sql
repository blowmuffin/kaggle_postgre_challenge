-- Return kaggle_challenge to its freshly generated state (destinations empty,
-- counters zero, identities restarted) without regenerating raw_data.
-- Run against kaggle_challenge; then run reset_control.sql against the control DB.
DROP SCHEMA IF EXISTS pgdm_expansion CASCADE;
TRUNCATE public.table1, public.table2, public.table3, public.table4,
         public.table5, public.table6 RESTART IDENTITY CASCADE;
DO $$
BEGIN
    IF to_regclass('public.pgdm_table_row_counts') IS NOT NULL THEN
        UPDATE public.pgdm_table_row_counts
        SET row_count = 0, updated_at = clock_timestamp()
        WHERE schema_name = 'public' AND table_name ~ '^table[1-6]$';
    END IF;
END $$;
