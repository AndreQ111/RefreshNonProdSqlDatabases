-- YourDatabaseDevShrinkReIndex.sql
USE [YourDatabaseDev]
GO
-- shrink database (5.5 hours)
DBCC SHRINKDATABASE(N'YourDatabaseDev' )
GO

-- reindex everything (1 hour)
EXECUTE master.[dbo].[IndexOptimize]
	@Databases = 'YourDatabaseDev',
	@LogToTable = 'Y'
