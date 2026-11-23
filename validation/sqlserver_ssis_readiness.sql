-- SSISDB deployment readiness check (READ-ONLY, SELECT only, no changes).
SET NOCOUNT ON;
PRINT '== Server ==';
SELECT @@SERVERNAME AS SERVER_NAME,
       CAST(SERVERPROPERTY('ProductVersion') AS varchar(30)) AS VERSION,
       CAST(SERVERPROPERTY('Edition') AS varchar(60)) AS EDITION,
       CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS int) AS WINDOWS_AUTH_ONLY;
PRINT '== Services (SQL Agent, etc.) ==';
SELECT servicename, status_desc, startup_type_desc, service_account FROM sys.dm_server_services;
PRINT '== This connection ==';
SELECT encrypt_option, auth_scheme, net_transport, local_tcp_port FROM sys.dm_exec_connections WHERE session_id = @@SPID;
PRINT '== SSISDB catalog ==';
SELECT name, state_desc, create_date FROM sys.databases WHERE name = N'SSISDB';
IF DB_ID(N'SSISDB') IS NOT NULL
BEGIN
    EXEC (N'SELECT property_name, property_value FROM SSISDB.catalog.catalog_properties');
    EXEC (N'SELECT f.name AS folder_name, p.name AS project_name, p.last_deployed_time FROM SSISDB.catalog.folders f LEFT JOIN SSISDB.catalog.projects p ON p.folder_id = f.folder_id');
END
ELSE
    PRINT 'SSISDB catalog NOT found on this instance.';
