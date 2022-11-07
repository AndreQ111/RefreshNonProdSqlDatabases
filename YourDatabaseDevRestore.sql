-- YourDatabaseDevRestore.sql
/*
JIRA	
	PBR-449
Description:
	Copy over latest YourDatabase from YourProdServer's weekly backup if not done recently
	Restore it as YourDatabaseDev on YourSandbox
	Code in SQL job "YourDatabaseDevRestore" on YourSandbox

History
	12/8/21	Andre Quitta	Created
*/
use master
exec sp_configure 'show advanced options', 1
RECONFIGURE
exec sp_configure 'xp_cmdshell', 1 
RECONFIGURE
go

	-- 14 minutes for backup
-- 1. delete backup if more than 7 days old
drop table if exists #Dir
create table #Dir (LineInfo nvarchar(max))

declare @YourDatabaseBak nvarchar(max)
declare @BackupDate date

insert into #Dir(LineInfo)
exec xp_cmdshell  "dir \\YourSandbox\SqlBackup$\YourDatabase.bak"

select * from #Dir
select	@YourDatabaseBak = LineInfo
from	#Dir
where	LineInfo like '%YourDatabase.bak%'

print @YourDatabaseBak

if @YourDatabaseBak is not null
begin
	set @BackupDate = convert(datetime2, left(@YourDatabaseBak,10))
	print 'checking date of most recent backup'

	-- 2. no need to take backup file if recent
	if @BackupDate < dateadd(dd, -7, getdate())
	begin
		print 'most recent backup on YourSandbox is older. Deleting and copying over.'
		exec xp_cmdshell "del \\YourSandbox\SqlBackup$\YourDatabase.bak"

		-- 3. Copy backup to Z drive of YourSandbox
		print 'starting robocopy'
		EXEC xp_cmdshell "robocopy \\YourProdServer\SqlBackup$ \\YourSandbox\SqlBackup$ YourDatabase.bak"
		print 'finished robocopy'
	end	-- if @BackupDate < dateadd(dd -7 getdate())
	else
		print 'Latest backup is still recent no need to copy over from YourProdServer'

end	-- if @YourDatabaseBak is not null
else
begin
	print 'Cannot find DatStore.bak on YourSandbox retrieving'
	print 'starting robocopy'
	EXEC xp_cmdshell "robocopy \\YourProdServer\SqlBackup$ \\YourSandbox\SqlBackup$ YourDatabase.bak"
	print 'finished robocopy'
end	-- if @YourDatabaseBak is not null

-- 3. Restore backup  to \datalakebackup instance as YourDatabaseDev change to Simple recovery mode
print '3. Restore backup'
USE [master]

RESTORE DATABASE [YourDatabaseDev] 
	FROM  DISK = N'Z:\SQLBackup\YourDatabase.BAK' 
	WITH  FILE = 1,  
	MOVE N'YourDatabase' TO N'c:\SQLData\YourDatabaseDev.mdf',  
	MOVE N'YourDatabase_log' TO N'c:\SQLLogs\YourDatabaseDev_log.ldf',  
	NOUNLOAD,  REPLACE,  STATS = 5

GO

exec sp_configure 'show advanced options', 1
RECONFIGURE
exec sp_configure 'xp_cmdshell', 0 
RECONFIGURE
exec sp_configure 'show advanced options', 0
RECONFIGURE
go

/*
USE [YourDatabaseDev]
ALTER DATABASE [YourDatabaseDev] MODIFY FILE (NAME='YourDatabase', NEWNAME='YourDatabaseDev')
ALTER DATABASE [YourDatabaseDev] MODIFY FILE (NAME='YourDatabase_log', NEWNAME='YourDatabaseDev_log')
ALTER DATABASE [YourDatabaseDev] SET RECOVERY SIMPLE WITH NO_WAIT
ALTER ROLE [db_owner] ADD MEMBER [YourDomain\YourActiveDirectoryGroup-Reader]
go
*/