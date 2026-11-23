/* =====================================================================
   SQL Agent job: DBL_ETL_BATCH_SQL_STG_30MIN
   Runs SSISDB package \SSISDB\DBL_ETL\DBLSSIS_ORACLE_ETL\ETL_BATCH_SQL_STG.dtsx
   with environment DBL_ETL\DEV, every 30 minutes, 24x7.
   - Run on SSIS_SERVER\SQL_INSTANCE (SSMS, Windows Authentication).
   - Creates job only (msdb). No data is loaded until the schedule fires.
   - SYNCHRONIZED=True: job step waits for the package, so job status = package status.
   - Agent never starts a second run of the same job while one is still running.
   ===================================================================== */
USE msdb;
SET NOCOUNT ON;

DECLARE @job_name  sysname = N'DBL_ETL_BATCH_SQL_STG_30MIN',
        @env_ref_id bigint, @cmd nvarchar(max), @job_id uniqueidentifier;

SELECT @env_ref_id = er.reference_id
FROM SSISDB.catalog.environment_references er
JOIN SSISDB.catalog.projects p ON p.project_id = er.project_id
JOIN SSISDB.catalog.folders  f ON f.folder_id  = p.folder_id
WHERE f.name = N'DBL_ETL' AND p.name = N'DBLSSIS_ORACLE_ETL'
  AND er.environment_name = N'DEV';

IF @env_ref_id IS NULL
    THROW 50001, 'Environment reference DBL_ETL\DEV not found on project DBLSSIS_ORACLE_ETL. Job NOT created.', 1;
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @job_name)
    THROW 50002, 'Job DBL_ETL_BATCH_SQL_STG_30MIN already exists. Nothing changed.', 1;

SET @cmd = N'/ISSERVER "\"\SSISDB\DBL_ETL\DBLSSIS_ORACLE_ETL\ETL_BATCH_SQL_STG.dtsx\"" '
         + N'/SERVER "\"' + @@SERVERNAME + N'\"" '
         + N'/ENVREFERENCE ' + CAST(@env_ref_id AS nvarchar(20)) + N' '
         + N'/Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 '
         + N'/Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True '
         + N'/CALLERINFO SQLAGENT /REPORTING E';

BEGIN TRANSACTION;
BEGIN TRY
    EXEC dbo.sp_add_job
         @job_name = @job_name, @enabled = 1,
         @description = N'DBL_DIGITAL.dbo.ETL_Batch_SQL_STG -> Oracle APPS.ETL_BATCH_SQL_STG. Append new BatchNO only (duplicate protection). Every 30 min, 24x7.',
         @owner_login_name = N'sa', @job_id = @job_id OUTPUT;

    EXEC dbo.sp_add_jobstep
         @job_id = @job_id, @step_name = N'Run ETL_BATCH_SQL_STG (SSISDB, env DEV)',
         @subsystem = N'SSIS', @command = @cmd, @database_name = N'master',
         @retry_attempts = 0, @on_success_action = 1, @on_fail_action = 2;

    EXEC dbo.sp_add_jobschedule
         @job_id = @job_id, @name = N'Every 30 minutes (24x7)', @enabled = 1,
         @freq_type = 4, @freq_interval = 1,          -- daily
         @freq_subday_type = 4, @freq_subday_interval = 30,  -- every 30 minutes
         @active_start_time = 0, @active_end_time = 235959;

    EXEC dbo.sp_add_jobserver @job_id = @job_id, @server_name = N'(local)';
    COMMIT;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;

-- Verification (read-only)
SELECT j.name, j.enabled, s.step_name, s.subsystem, s.command
FROM msdb.dbo.sysjobs j JOIN msdb.dbo.sysjobsteps s ON s.job_id = j.job_id
WHERE j.name = @job_name;

SELECT j.name, js.next_run_date, js.next_run_time
FROM msdb.dbo.sysjobs j JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id
WHERE j.name = @job_name;
