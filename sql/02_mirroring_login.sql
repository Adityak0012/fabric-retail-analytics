/* =====================================================================
   Login + permissions Fabric Mirroring uses to read RetailOLTP.
   Source: Microsoft Learn "Tutorial: Configure Microsoft Fabric Mirroring
   from SQL Server" (SQL Server 2016-2022 section).

   Before running:
     * Enable "SQL Server and Windows Authentication mode"
       (SSMS > server Properties > Security) and restart the service.
     * Make sure the SQL Server Agent service is RUNNING and set to
       Automatic. Mirroring on 2016-2022 is built on Change Data Capture,
       and CDC capture/cleanup jobs run as Agent jobs.
   ===================================================================== */
USE [master];
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'fabric_login')
    CREATE LOGIN [fabric_login] WITH PASSWORD = 'Change-Me-Str0ng!Pass', CHECK_POLICY = ON;
GO

USE RetailOLTP;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'fabric_user')
    CREATE USER [fabric_user] FOR LOGIN [fabric_login];
GO
GRANT SELECT, ALTER ANY EXTERNAL MIRROR,
      VIEW DATABASE PERFORMANCE STATE, VIEW DATABASE SECURITY STATE
TO [fabric_user];
GO

/* Sanity checks after you start mirroring in Fabric --------------------
   CDC should show as enabled on the DB and each mirrored table:        */
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'RetailOLTP';
SELECT name, is_tracked_by_cdc FROM sys.tables ORDER BY name;
/* If Fabric reports a permissions error while enabling CDC, check the
   latest Microsoft Learn tutorial for your SQL Server version — required
   permissions have changed between releases. As a last resort on a
   throwaway dev box you can temporarily run:
       ALTER ROLE db_owner ADD MEMBER [fabric_user];
   and remove it once mirroring is running.                               */
