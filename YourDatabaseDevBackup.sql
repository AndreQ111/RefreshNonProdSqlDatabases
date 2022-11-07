-- YourDatabaseDevBackup.sql
	-- 25 minutes
BACKUP DATABASE [YourDatabaseDev] 
	TO  DISK = N'Z:\SQLBackup\YourDatabaseDev.bak' 
	WITH NOFORMAT, INIT,  
	NAME = N'YourDatabaseDev-Full Database Backup', 
	SKIP, NOREWIND, NOUNLOAD,  STATS = 10

go
WAITFOR DELAY '00:30:00'; -- 30 minutes

USE [master]
GO
DROP DATABASE [YourDatabaseDev]
GO

