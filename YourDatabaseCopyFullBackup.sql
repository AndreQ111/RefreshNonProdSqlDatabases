-- YourDatabaseCopyFullBackup.sql
/*
takes 9 minutes time

Copy the most recent full backup of YourDatabase 
	from u:\SqlBackup\ProdServer\YourDatabase\Full 
	to u:\SqlBackup\YourDatabase.back

1/28/22		Andre Quitta	Created
1/30/22		Andre Quitta	Added /Y to DOS COPY command to overwrite previous backup.
*/

set nocount on

exec sp_configure 'show advanced options', '1'
RECONFIGURE
exec sp_configure 'xp_cmdshell', '1' 
RECONFIGURE

drop table if exists #Directory

create table #Directory (
	DirOutput varchar(1000)
)
go

declare @SourceFolder nvarchar(200) = 'u:\SqlBackup\SourceServer\YourDatabase\Full' 
declare @Cmd nvarchar(200) = 'Dir '+@SourceFolder+'\*.bak'
print @Cmd
insert into #Directory (DirOutput)
EXEC xp_cmdshell @Cmd

delete
from	#Directory
where	(DirOutput not like '%YourDatabase%' and DirOutput not like '%.bak%')
	or	DirOutput like '%Directory%'
	or	DirOutput is null

if (select count(*) from #Directory) > 1
	delete
	from	#Directory
	where	DirOutput <> (	select	max(DirOutput)
							from	#Directory
							)


declare @BackupFile nvarchar(200)

select	@BackupFile = rtrim(SUBSTRING(DirOutput, charindex(@@SERVERNAME, DirOutput),100))
from	#Directory


set @Cmd = 'del u:\SqlBackup\YourDatabase.bak'
print @Cmd
EXEC xp_cmdshell @Cmd

set @Cmd = 'copy '+@SourceFolder+'\'+@BackupFile+' u:\SqlBackup\YourDatabase.bak /Y'
print @Cmd
EXEC xp_cmdshell @Cmd


exec sp_configure 'show advanced options', '1'
RECONFIGURE
exec sp_configure 'xp_cmdshell', '0'
RECONFIGURE
exec sp_configure 'show advanced options', '0'
RECONFIGURE

set nocount off
