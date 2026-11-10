-- Remove expansion versions of the challenge destinations from the control DB
-- (keeps the generator's initial 0000 versions). Run against postgres_data_manager.
DELETE FROM app_control.table_versions
WHERE database_name = 'kaggle_challenge'
  AND schema_name = 'public'
  AND table_name ~ '^table[1-6]$'
  AND version_code <> '0000';
DO $$
BEGIN
    IF to_regclass('app_control.expansion_job_registrations') IS NOT NULL THEN
        DELETE FROM app_control.expansion_job_registrations
        WHERE database_name = 'kaggle_challenge';
    END IF;
END $$;
