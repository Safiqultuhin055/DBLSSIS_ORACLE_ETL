/* =====================================================================
   Monitoring: DBL_ETL_BATCH_SQL_STG_30MIN  (READ-ONLY, SELECT only)
   Run on SSIS_SERVER\SQL_INSTANCE in SSMS (Windows Authentication).
   Change @hours to look further back (default: last 24 hours).

   Result sets:
     1. NOW            - running now? last outcome, next scheduled run
     2. JOB RUNS       - each run: start/end/duration, Step 1 (procedure), Step 2 (SSIS)
     3. SSIS RUNS      - each SSISDB execution + rows written to Oracle / reject files
     4. SSIS ERRORS    - error/warning messages for failed executions in the window
     5. SOURCE DAILY   - DBL_DIGITAL.dbo.ETL_Batch_SQL_STG rows per BatchDate (last 7 days)
   ===================================================================== */
SET NOCOUNT ON;

DECLARE @job_name sysname = N'DBL_ETL_BATCH_SQL_STG_30MIN',
        @hours    int     = 24;
DECLARE @since datetime = DATEADD(HOUR, -@hours, GETDATE());

/* ---------- 1. NOW ---------- */
SELECT  j.name                                        AS job_name,
        CASE WHEN j.enabled = 1 THEN 'Enabled' ELSE 'DISABLED' END AS job_enabled,
        CASE WHEN a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
             THEN 'RUNNING since ' + CONVERT(varchar(19), a.start_execution_date, 120)
             ELSE 'Idle' END                          AS current_state,
        a.last_executed_step_id                       AS last_step_done,
        CONVERT(varchar(19), a.start_execution_date, 120) AS last_start,
        CONVERT(varchar(19), a.stop_execution_date, 120)  AS last_stop,
        CASE lh.run_status WHEN 0 THEN 'FAILED' WHEN 1 THEN 'Succeeded' WHEN 2 THEN 'Retry'
                           WHEN 3 THEN 'Canceled' WHEN 4 THEN 'In progress' END AS last_outcome,
        CONVERT(varchar(19), a.next_scheduled_run_date, 120) AS next_run
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobactivity a
       ON a.job_id = j.job_id
      AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)
LEFT JOIN msdb.dbo.sysjobhistory lh ON lh.instance_id = a.job_history_id
WHERE j.name = @job_name;

/* ---------- 2. JOB RUNS (one row per run, newest first) ---------- */
WITH h AS (
    SELECT h.instance_id, h.step_id, h.run_status, h.message,
           msdb.dbo.agent_datetime(h.run_date, h.run_time) AS start_dt,
           (h.run_duration / 10000) * 3600 + (h.run_duration / 100 % 100) * 60
             + (h.run_duration % 100)                     AS dur_s
    FROM msdb.dbo.sysjobhistory h
    JOIN msdb.dbo.sysjobs j ON j.job_id = h.job_id
    WHERE j.name = @job_name
), o AS (   -- step_id 0 = job outcome row, written after its steps
    SELECT instance_id, start_dt, dur_s, run_status, message,
           LAG(instance_id, 1, 0) OVER (ORDER BY instance_id) AS prev_id
    FROM h WHERE step_id = 0
)
SELECT  CONVERT(varchar(19), o.start_dt, 120)                           AS job_start,
        CONVERT(varchar(19), DATEADD(SECOND, o.dur_s, o.start_dt), 120) AS job_end,
        o.dur_s                                                         AS job_sec,
        CASE o.run_status WHEN 0 THEN 'FAILED' WHEN 1 THEN 'Succeeded'
                          WHEN 3 THEN 'Canceled' ELSE CAST(o.run_status AS varchar(5)) END AS job_outcome,
        CASE s1.run_status WHEN 0 THEN 'FAILED' WHEN 1 THEN 'OK' END   AS step1_proc,
        s1.dur_s                                                        AS step1_sec,
        CASE s2.run_status WHEN 0 THEN 'FAILED' WHEN 1 THEN 'OK'
             ELSE CASE WHEN s1.run_status = 0 THEN 'skipped' END END    AS step2_ssis,
        s2.dur_s                                                        AS step2_sec,
        CASE WHEN o.run_status <> 1
             THEN LEFT(COALESCE(CASE WHEN s1.run_status = 0 THEN s1.message END,
                                CASE WHEN s2.run_status = 0 THEN s2.message END,
                                o.message), 400) END                    AS failure_message
FROM o
OUTER APPLY (SELECT TOP (1) run_status, dur_s, message FROM h
             WHERE h.step_id = 1 AND h.instance_id > o.prev_id AND h.instance_id < o.instance_id
             ORDER BY h.instance_id DESC) s1
OUTER APPLY (SELECT TOP (1) run_status, dur_s, message FROM h
             WHERE h.step_id = 2 AND h.instance_id > o.prev_id AND h.instance_id < o.instance_id
             ORDER BY h.instance_id DESC) s2
WHERE o.start_dt >= @since
ORDER BY o.start_dt DESC;

/* ---------- 3. SSIS RUNS + rows written ---------- */
WITH e AS (
    SELECT execution_id, status, executed_as_name, caller_name,
           CONVERT(datetime2(0), start_time) AS start_t,
           CONVERT(datetime2(0), end_time)   AS end_t
    FROM SSISDB.catalog.executions
    WHERE folder_name  = N'DBL_ETL'
      AND project_name = N'DBLSSIS_ORACLE_ETL'
      AND package_name = N'ETL_BATCH_SQL_STG.dtsx'
      AND start_time  >= @since
), msg AS (   -- only '"<component>" wrote N rows.' information messages
    SELECT m.operation_id AS execution_id, m.message
    FROM SSISDB.catalog.event_messages m
    WHERE m.operation_id IN (SELECT execution_id FROM e)
      AND m.message LIKE N'"%" wrote % row%'
), w AS (
    -- CASE guards: SQL Server may evaluate SUBSTRING before the LIKE filter,
    -- which could raise "Invalid length parameter" on other messages.
    SELECT execution_id,
           CASE WHEN message LIKE N'"%" wrote % row%'
                THEN SUBSTRING(message, 2, CHARINDEX('"', message, 2) - 2) END AS component,
           CASE WHEN message LIKE N'"%" wrote % row%'
                THEN TRY_CAST(SUBSTRING(message, CHARINDEX(' wrote ', message) + 7,
                         CHARINDEX(' row', message, CHARINDEX(' wrote ', message))
                         - CHARINDEX(' wrote ', message) - 7) AS int) END AS rows_written
    FROM msg
)
SELECT  e.execution_id,
        CONVERT(varchar(19), e.start_t, 120)  AS ssis_start,
        CONVERT(varchar(19), e.end_t, 120)    AS ssis_end,
        DATEDIFF(SECOND, e.start_t, e.end_t)  AS ssis_sec,
        CASE e.status WHEN 1 THEN 'Created' WHEN 2 THEN 'RUNNING' WHEN 3 THEN 'Canceled'
                      WHEN 4 THEN 'FAILED' WHEN 5 THEN 'Pending' WHEN 6 THEN 'Ended unexpectedly'
                      WHEN 7 THEN 'Succeeded' WHEN 8 THEN 'Stopping' WHEN 9 THEN 'Completed' END AS ssis_status,
        SUM(CASE WHEN w.component = 'OLE_DST_ORACLE_ETL_BATCH_SQL_STG' THEN w.rows_written END) AS inserted_to_oracle,
        SUM(CASE WHEN w.component = 'FF_REJECTED_INSERT_ERRORS'        THEN w.rows_written END) AS rejected_insert,
        SUM(CASE WHEN w.component = 'FF_REJECTED_INVALID_KEY'          THEN w.rows_written END) AS rejected_invalid_key,
        SUM(CASE WHEN w.component = 'FF_PENDING_CORRECTIONS'           THEN w.rows_written END) AS pending_corrections,
        e.executed_as_name, e.caller_name
FROM e
LEFT JOIN w ON w.execution_id = e.execution_id
GROUP BY e.execution_id, e.start_t, e.end_t, e.status, e.executed_as_name, e.caller_name
ORDER BY e.execution_id DESC;

/* ---------- 4. SSIS ERRORS / WARNINGS (failed executions only) ---------- */
SELECT  m.operation_id                                AS execution_id,
        CONVERT(varchar(19), CONVERT(datetime2(0), m.message_time), 120) AS message_time,
        CASE m.message_type WHEN 120 THEN 'ERROR' WHEN 110 THEN 'Warning' END AS msg_type,
        m.message_source_name,
        LEFT(m.message, 500)                          AS message
FROM SSISDB.catalog.event_messages m
JOIN SSISDB.catalog.executions x ON x.execution_id = m.operation_id
WHERE x.folder_name  = N'DBL_ETL'
  AND x.project_name = N'DBLSSIS_ORACLE_ETL'
  AND x.package_name = N'ETL_BATCH_SQL_STG.dtsx'
  AND x.start_time  >= @since
  AND x.status IN (3, 4, 6)
  AND m.message_type IN (110, 120)
ORDER BY m.operation_id DESC, m.message_time;

/* ---------- 5. SOURCE DAILY (what the procedure brought in) ---------- */
SELECT  CAST(BatchDate AS date) AS batch_date,
        COUNT(*)                AS source_rows
FROM DBL_DIGITAL.dbo.ETL_Batch_SQL_STG
WHERE BatchDate >= DATEADD(DAY, -7, CAST(GETDATE() AS date))
GROUP BY CAST(BatchDate AS date)
ORDER BY batch_date DESC;
