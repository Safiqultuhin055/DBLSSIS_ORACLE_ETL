-- Phase A: SQL Server source data profile (READ-ONLY: SELECT statements only)
-- Run:
--   sqlcmd -S SQLSERVER_HOST -U SQL_LOGIN -d DBL_DIGITAL -i "E:\DBL SSIS WHTH ORACLE ETL\validation\sqlserver_phaseA_profile.sql" -o "E:\DBL SSIS WHTH ORACLE ETL\validation\out_sqlserver_phaseA_profile.txt" -s "|" -W
-- Output contains no credentials. Non-ASCII check = any character outside printable ASCII (space..~).

SET NOCOUNT ON;

PRINT '===NULL_COUNTS';
SELECT COUNT_BIG(*) AS TOTAL_ROWS,
  SUM(CASE WHEN BatchDate           IS NULL THEN 1 ELSE 0 END) AS BatchDate,
  SUM(CASE WHEN OrderFullNo         IS NULL THEN 1 ELSE 0 END) AS OrderFullNo,
  SUM(CASE WHEN UnitName            IS NULL THEN 1 ELSE 0 END) AS UnitName,
  SUM(CASE WHEN ColorName           IS NULL THEN 1 ELSE 0 END) AS ColorName,
  SUM(CASE WHEN BatchNO             IS NULL THEN 1 ELSE 0 END) AS BatchNO,
  SUM(CASE WHEN FabricDia           IS NULL THEN 1 ELSE 0 END) AS FabricDia,
  SUM(CASE WHEN FabricGSM           IS NULL THEN 1 ELSE 0 END) AS FabricGSM,
  SUM(CASE WHEN OtherQty            IS NULL THEN 1 ELSE 0 END) AS OtherQty,
  SUM(CASE WHEN Rolls               IS NULL THEN 1 ELSE 0 END) AS Rolls,
  SUM(CASE WHEN BuyerShortName      IS NULL THEN 1 ELSE 0 END) AS BuyerShortName,
  SUM(CASE WHEN Item                IS NULL THEN 1 ELSE 0 END) AS Item,
  SUM(CASE WHEN OrderCustomerType   IS NULL THEN 1 ELSE 0 END) AS OrderCustomerType,
  SUM(CASE WHEN BatchQty            IS NULL THEN 1 ELSE 0 END) AS BatchQty,
  SUM(CASE WHEN BatchGreyQty        IS NULL THEN 1 ELSE 0 END) AS BatchGreyQty,
  SUM(CASE WHEN CustomerTypeQty     IS NULL THEN 1 ELSE 0 END) AS CustomerTypeQty,
  SUM(CASE WHEN FabricTypeName      IS NULL THEN 1 ELSE 0 END) AS FabricTypeName,
  SUM(CASE WHEN DyeingStartDate     IS NULL THEN 1 ELSE 0 END) AS DyeingStartDate,
  SUM(CASE WHEN TimelyBatchOK       IS NULL THEN 1 ELSE 0 END) AS TimelyBatchOK,
  SUM(CASE WHEN GreyReceiveTypeName IS NULL THEN 1 ELSE 0 END) AS GreyReceiveTypeName,
  SUM(CASE WHEN PrdFloor            IS NULL THEN 1 ELSE 0 END) AS PrdFloor
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===MAX_CHAR_LENGTHS (actual data, trailing spaces included)';
SELECT
  MAX(DATALENGTH(OrderFullNo))/2         AS OrderFullNo_100,
  MAX(DATALENGTH(UnitName))/2            AS UnitName_100,
  MAX(DATALENGTH(ColorName))/2           AS ColorName_150,
  MAX(DATALENGTH(BatchNO))/2             AS BatchNO_50,
  MAX(DATALENGTH(FabricDia))/2           AS FabricDia_50,
  MAX(DATALENGTH(FabricGSM))/2           AS FabricGSM_50,
  MAX(DATALENGTH(BuyerShortName))/2      AS BuyerShortName_150,
  MAX(DATALENGTH(Item))/2                AS Item_150,
  MAX(DATALENGTH(OrderCustomerType))/2   AS OrderCustomerType_50,
  MAX(DATALENGTH(FabricTypeName))/2      AS FabricTypeName_150,
  MAX(DATALENGTH(GreyReceiveTypeName))/2 AS GreyReceiveTypeName_150,
  MAX(DATALENGTH(PrdFloor))/2            AS PrdFloor_100
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===NON_ASCII_ROW_COUNTS';
SELECT
  SUM(CASE WHEN OrderFullNo         COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS OrderFullNo,
  SUM(CASE WHEN UnitName            COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS UnitName,
  SUM(CASE WHEN ColorName           COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS ColorName,
  SUM(CASE WHEN BatchNO             COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS BatchNO,
  SUM(CASE WHEN FabricDia           COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS FabricDia,
  SUM(CASE WHEN FabricGSM           COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS FabricGSM,
  SUM(CASE WHEN BuyerShortName      COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS BuyerShortName,
  SUM(CASE WHEN Item                COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS Item,
  SUM(CASE WHEN OrderCustomerType   COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS OrderCustomerType,
  SUM(CASE WHEN FabricTypeName      COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS FabricTypeName,
  SUM(CASE WHEN GreyReceiveTypeName COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS GreyReceiveTypeName,
  SUM(CASE WHEN PrdFloor            COLLATE Latin1_General_BIN LIKE N'%[^ -~]%' THEN 1 ELSE 0 END) AS PrdFloor
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===TIMELYBATCHOK_VALUES';
SELECT ISNULL(TimelyBatchOK, '<NULL>') AS TimelyBatchOK, COUNT_BIG(*) AS ROWS_
FROM dbo.ETL_Batch_SQL_STG GROUP BY TimelyBatchOK ORDER BY TimelyBatchOK;

PRINT '===NUMERIC_RANGES';
SELECT MIN(OtherQty) AS OtherQty_MIN, MAX(OtherQty) AS OtherQty_MAX,
       MIN(Rolls) AS Rolls_MIN, MAX(Rolls) AS Rolls_MAX,
       MIN(BatchQty) AS BatchQty_MIN, MAX(BatchQty) AS BatchQty_MAX,
       MIN(BatchGreyQty) AS BatchGreyQty_MIN, MAX(BatchGreyQty) AS BatchGreyQty_MAX,
       MIN(CustomerTypeQty) AS CustomerTypeQty_MIN, MAX(CustomerTypeQty) AS CustomerTypeQty_MAX
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===DATE_RANGES';
SELECT MIN(BatchDate) AS BatchDate_MIN, MAX(BatchDate) AS BatchDate_MAX,
       COUNT(DISTINCT BatchDate) AS BatchDate_DISTINCT,
       MIN(DyeingStartDate) AS DyeingStart_MIN, MAX(DyeingStartDate) AS DyeingStart_MAX
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===DUPLICATES_FULL_ROW (groups of identical 20-column rows)';
SELECT COUNT(*) AS DUP_GROUPS, ISNULL(SUM(CNT),0) AS ROWS_IN_DUP_GROUPS
FROM (SELECT COUNT(*) AS CNT FROM dbo.ETL_Batch_SQL_STG
      GROUP BY BatchDate, OrderFullNo, UnitName, ColorName, BatchNO, FabricDia, FabricGSM,
               OtherQty, Rolls, BuyerShortName, Item, OrderCustomerType, BatchQty, BatchGreyQty,
               CustomerTypeQty, FabricTypeName, DyeingStartDate, TimelyBatchOK, GreyReceiveTypeName, PrdFloor
      HAVING COUNT(*) > 1) d;

PRINT '===KEY_CANDIDATES (distinct counts vs total rows)';
SELECT COUNT_BIG(*) AS TOTAL_ROWS,
       COUNT(DISTINCT BatchNO) AS DISTINCT_BatchNO,
       (SELECT COUNT(*) FROM (SELECT DISTINCT BatchDate, BatchNO FROM dbo.ETL_Batch_SQL_STG) x) AS DISTINCT_BatchDate_BatchNO,
       (SELECT COUNT(*) FROM (SELECT DISTINCT BatchNO, OrderFullNo, ColorName FROM dbo.ETL_Batch_SQL_STG) x) AS DISTINCT_BatchNO_Order_Color,
       (SELECT COUNT(*) FROM (SELECT DISTINCT BatchDate, BatchNO, OrderFullNo, ColorName, FabricTypeName FROM dbo.ETL_Batch_SQL_STG) x) AS DISTINCT_Date_Batch_Order_Color_Fabric
FROM dbo.ETL_Batch_SQL_STG;

PRINT '===TOP_DUPLICATE_BATCHNO';
SELECT TOP 5 BatchNO, COUNT_BIG(*) AS ROWS_
FROM dbo.ETL_Batch_SQL_STG GROUP BY BatchNO HAVING COUNT_BIG(*) > 1 ORDER BY COUNT_BIG(*) DESC;

PRINT '===SOURCE_OBJECT_INFO';
SELECT o.type_desc AS OBJECT_TYPE, o.create_date, o.modify_date,
       (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id = o.object_id AND i.type > 0) AS INDEX_COUNT,
       (SELECT COUNT(*) FROM sys.triggers t WHERE t.parent_id = o.object_id) AS TRIGGER_COUNT
FROM sys.objects o WHERE o.object_id = OBJECT_ID(N'dbo.ETL_Batch_SQL_STG');

PRINT '===END';
