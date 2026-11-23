/* =====================================================================
   Adds a first step to job DBL_ETL_BATCH_SQL_STG_30MIN:
     Step 1: EXEC DBL_DIGITAL.dbo.SP_ETL_Batch_SQL_STG   (refresh source staging)
     Step 2: SSIS package ETL_BATCH_SQL_STG               (existing step, shifted)
   Step 1 fails -> job stops, SSIS step does NOT run.
   Run on SSIS_SERVER\SQL_INSTANCE only AFTER the procedure has been reviewed
   (it must not create duplicate BatchNO rows in dbo.ETL_Batch_SQL_STG).
   ===================================================================== */
USE msdb;
SET NOCOUNT ON;

DECLARE @job_name sysname = N'DBL_ETL_BATCH_SQL_STG_30MIN';

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @job_name)
    THROW 50011, 'Job DBL_ETL_BATCH_SQL_STG_30MIN not found. Nothing changed.', 1;
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs j JOIN msdb.dbo.sysjobsteps s ON s.job_id = j.job_id
           WHERE j.name = @job_name AND s.subsystem = N'TSQL' AND s.command LIKE N'%SP_ETL_Batch_SQL_STG%')
    THROW 50012, 'Procedure step already exists on the job. Nothing changed.', 1;
IF OBJECT_ID(N'DBL_DIGITAL.dbo.SP_ETL_Batch_SQL_STG', N'P') IS NULL
    THROW 50013, 'Procedure DBL_DIGITAL.dbo.SP_ETL_Batch_SQL_STG not found. Nothing changed.', 1;

BEGIN TRANSACTION;
BEGIN TRY
    -- Insert as step 1; the existing SSIS step becomes step 2.
    EXEC dbo.sp_add_jobstep
         @job_name = @job_name, @step_id = 1,
         @step_name = N'Refresh source staging (SP_ETL_Batch_SQL_STG)',
         @subsystem = N'TSQL', @database_name = N'DBL_DIGITAL',
         @command = N'EXEC dbo.SP_ETL_Batch_SQL_STG;',
         @retry_attempts = 0,
         @on_success_action = 3,   -- go to next step (SSIS)
         @on_fail_action = 2;      -- quit reporting failure (SSIS step skipped)

    EXEC dbo.sp_update_job @job_name = @job_name, @start_step_id = 1;
    COMMIT;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;

-- Verification (read-only)
SELECT s.step_id, s.step_name, s.subsystem, s.database_name,
       s.on_success_action, s.on_fail_action, LEFT(s.command, 120) AS command_start
FROM msdb.dbo.sysjobs j JOIN msdb.dbo.sysjobsteps s ON s.job_id = j.job_id
WHERE j.name = @job_name
ORDER BY s.step_id;
