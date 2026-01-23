-- SQL Server Connection Test Query
-- This query validates the database connection and retrieves basic environment information

-- Get SQL Server version
SELECT @@VERSION AS SqlServerVersion;

-- Get current database name
SELECT DB_NAME() AS CurrentDatabase;

-- Get current timestamp
SELECT GETDATE() AS CurrentTime;

-- Get current user
SELECT SYSTEM_USER AS CurrentUser;
