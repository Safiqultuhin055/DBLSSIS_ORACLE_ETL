-- Step 2: SQL Server source validation (READ-ONLY: SELECT statements only)
-- Run from this folder (same server address as CM_SQLSERVER_DBL_DIGITAL):
--   sqlcmd -S SQLSERVER_HOST -U SQL_LOGIN -d DBL_DIGITAL -i sqlserver_step2_metadata.sql -o out_sqlserver_step2.txt -s "|" -W
-- sqlcmd prompts for the password. Output contains no credentials.

SET NOCOUNT ON;

PRINT '===SQL_IDENTITY';
SELECT @@SERVERNAME                                              AS SERVER_NAME,
       CAST(SERVERPROPERTY('InstanceName') AS nvarchar(128))     AS INSTANCE_NAME,
       DB_NAME()                                                 AS DATABASE_NAME,
       SUSER_SNAME()                                             AS LOGIN_NAME,
       CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50))    AS PRODUCT_VERSION,
       CAST(SERVERPROPERTY('Edition') AS nvarchar(128))          AS EDITION,
       CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS nvarchar(128)) AS DB_COLLATION;

PRINT '===SQL_COLUMNS';
SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
       NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE, COLLATION_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'ETL_Batch_SQL_STG'
ORDER BY ORDINAL_POSITION;

PRINT '===SQL_ROWCOUNT';
SELECT COUNT_BIG(*) AS SOURCE_ROWS FROM dbo.ETL_Batch_SQL_STG;

PRINT '===SQL_DATE_PRECISION';
-- Rows whose DATETIME values carry milliseconds (Oracle DATE keeps whole seconds only)
SELECT SUM(CASE WHEN DATEPART(millisecond, BatchDate) <> 0 THEN 1 ELSE 0 END)       AS BATCHDATE_WITH_MS,
       SUM(CASE WHEN DATEPART(millisecond, DyeingStartDate) <> 0 THEN 1 ELSE 0 END) AS DYEINGSTART_WITH_MS,
       MIN(BatchDate) AS MIN_BATCHDATE, MAX(BatchDate) AS MAX_BATCHDATE
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===END';
