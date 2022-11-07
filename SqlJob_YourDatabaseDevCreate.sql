-- SqlJob_YourDatabaseDevCreate.sql
USE [msdb]
GO

/****** Object:  Job [YourDatabaseDevCreate]   ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 10/28/2022 2:55:06 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'YourDatabaseDevCreate', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Create YourDatabaseDev based off of YourDatabase Prod backup.
(13 hours)
', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'SERVICES\YourServiceAccount', 
		@notify_email_operator_name=N'SqlAlert', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [drop YourDatabaseDev if exists]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'drop YourDatabaseDev if exists', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if (select count(*) from sys.databases where name = ''YourDatabaseDev'' ) > 0
begin
     ALTER DATABASE [YourDatabaseDev] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
     DROP DATABASE [YourDatabaseDev]
end

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [YourDatabaseDevRestore.sql]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'YourDatabaseDevRestore.sql', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- YourDatabaseDevRestore.sql
/*
Description:
	Copy over latest YourDatabase from YourProdServers weekly backup if not done recently
	Restore it as YourDatabaseDev on YourSandboxServer
	Code in SQL job "YourDatabaseDevRestore" on YourSandboxServer

History
	12/8/21	Andre Quitta	Created
*/
use master
exec sp_configure ''show advanced options'', 1
RECONFIGURE
exec sp_configure ''xp_cmdshell'', 1 
RECONFIGURE
go

	-- 14 minutes for backup
-- 1. delete backup if more than 7 days old
drop table if exists #Dir
create table #Dir (LineInfo nvarchar(max))

declare @YourDatabaseBak nvarchar(max)
declare @BackupDate date

insert into #Dir(LineInfo)
exec xp_cmdshell  "dir \\YourSandboxServer\SqlBackup$\YourDatabase.bak"

select * from #Dir
select	@YourDatabaseBak = LineInfo
from	#Dir
where	LineInfo like ''%YourDatabase.bak%''

print @YourDatabaseBak

if @YourDatabaseBak is not null
begin
	set @BackupDate = convert(datetime2, left(@YourDatabaseBak,10))
	print ''checking date of most recent backup''

	-- 2. no need to take backup file if recent
	if @BackupDate < dateadd(dd, -7, getdate())
	begin
		print ''most recent backup on YourSandboxServer is older. Deleting and copying over.''
		exec xp_cmdshell "del \\YourSandboxServer\SqlBackup$\YourDatabase.bak"

		-- 3. Copy backup to Z drive of YourSandboxServer
		print ''starting robocopy''
		EXEC xp_cmdshell "robocopy \\YourProdServer\SqlBackup$ \\YourSandboxServer\SqlBackup$ YourDatabase.bak"
		print ''finished robocopy''
	end	-- if @BackupDate < dateadd(dd -7 getdate())
	else
		print ''Latest backup is still recent no need to copy over from YourProdServer''

end	-- if @YourDatabaseBak is not null
else
begin
	print ''Cannot find YourDatabase.bak on YourSandboxServer retrieving''
	print ''starting robocopy''
	EXEC xp_cmdshell "robocopy \\YourProdServer\SqlBackup$ \\YourSandboxServer\SqlBackup$ YourDatabase.bak"
	print ''finished robocopy''
end	-- if @YourDatabaseBak is not null

-- 3. Restore backup  to \YourDatabasebackup instance as YourDatabaseDev change to Simple recovery mode
print ''3. Restore backup''
USE [master]

RESTORE DATABASE [YourDatabaseDev] 
	FROM  DISK = N''c:\SQLBackup\YourDatabase.BAK'' 
	WITH  FILE = 1,  
	MOVE N''YourDatabase'' TO N''c:\SQLData\YourDatabaseDev.mdf'',  
	MOVE N''YourDatabase_log'' TO N''c:\SQLLogs\YourDatabaseDev_log.ldf'',  
	NOUNLOAD,  REPLACE,  STATS = 5

GO

exec sp_configure ''show advanced options'', 1
RECONFIGURE
exec sp_configure ''xp_cmdshell'', 0 
RECONFIGURE
exec sp_configure ''show advanced options'', 0
RECONFIGURE
go
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [ALTER DATABASE [YourDatabaseDev]]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ALTER DATABASE [YourDatabaseDev]', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE [YourDatabaseDev]
ALTER DATABASE [YourDatabaseDev] MODIFY FILE (NAME=''YourDatabase'', NEWNAME=''YourDatabaseDev'')
ALTER DATABASE [YourDatabaseDev] MODIFY FILE (NAME=''YourDatabase_log'', NEWNAME=''YourDatabaseDev_log'')
ALTER DATABASE [YourDatabaseDev] SET RECOVERY SIMPLE WITH NO_WAIT
ALTER ROLE [db_owner] ADD MEMBER [YourDomain\YourDatabase-Reader]
go
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [YourDatabaseDevTrim.sql]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'YourDatabaseDevTrim.sql', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- YourDatabaseDevTrim.sql

/*
Description:
	delete records in the Data tables that are older than 1 month and have a date column that''s named like ''occurred''
		done by copying 1 month''s of data to a 2nd table and renaming it back to the original name
		including identity columns, PKs and indices

	tables list, from original Current/Archve:
		Table												Column split	Status					To be Trimmed
		------------------------------						------------	----------				--------------
		dbo.HasFinancialInfo								ColumnToBeMasked
		dbo.HasFinancialInfo2								Column2ToBeMasked

	Code in SQL job "YourDatabaseDevCreate" on YourSandboxServer
Dependency:
	dev.IndexDefinition
	dev.TableDefinition
History
	12/8/21		Andre Quitta	Created
	12/9/21		Andre Quitta	Dynamic table collection, deleting records
	12/10/21	Andre Quitta	Some tables are not indexed on date, looking to leverage IDs instead and custom queries
	1/7/22		Andre Quitta	sp_rename fixed, indices listed in separate routine
	2/15/22		Andre Quitta	exec sp_executesql @DropOneMonthTable, corrected comments
	2/22/22		Andre Quitta	removed GO; 
	3/1/22		Andre Quitta	updated to DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0) for 1st day previous month
	3/9/22		Andre Qutita	blanked out @IxDef in the beginning of WHILE statement
								deleting any existing table that has ''OneMonth'' suffix
	3/24/22		Andre, Genessa	Adding column PostDateTime to 10 million record table WHERE

*/
USE [YourDatabaseDev]

-- ensure any previous ''OneMonth'' tables are removed, allowing for safe repeated executions
DROP TABLE IF EXISTS #DeleteOneMonthTables

SELECT	s.name + ''.'' + t.name as TableName,
		0 as Done
INTO	#DeleteOneMonthTables
FROM	sys.tables t
JOIN	sys.schemas s on s.schema_id = t.schema_id
WHERE	t.name like ''%OneMonth''

DECLARE @DeleteOneMonthSql NVARCHAR(2000)
DECLARE @OneMonthTable NVARCHAR(200)
WHILE (SELECT COUNT(*) FROM #DeleteOneMonthTables WHERE Done = 0)  > 0
BEGIN
	SELECT	@OneMonthTable = MIN(TableName)
	FROM	#DeleteOneMonthTables WHERE Done = 0

	SET @DeleteOneMonthSql = ''DROP TABLE '' + @OneMonthTable
	PRINT @DeleteOneMonthSql
	exec sp_executesql @DeleteOneMonthSql

	UPDATE	#DeleteOneMonthTables
	SET		Done = 1
	WHERE	TableName = @DeleteOneMonthSql
END


SET NOCOUNT ON
declare @Million int = 1000000
drop table if exists #Table10Mil

-- tables with over 10 million
SELECT	SCHEMA_NAME(sOBJ.schema_id) + ''.'' + sOBJ.name AS [TableName]
		, sOBJ.name as TableNoSchema
		, min(sOBJ.object_id) as ObjectID
		, SUM(sPTN.Rows) AS [RowCount]
		, 0 as Done
INTO	#Table10Mil
FROM	sys.objects AS sOBJ
INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id
WHERE	sOBJ.type = ''U''
	AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2 -- 0:Heap, 1:Clustered
GROUP BY sOBJ.schema_id
		, sOBJ.name
HAVING	SUM(sPTN.Rows) > 10 * @Million
ORDER BY SUM(sPTN.Rows) DESC

--select	* from #Table10Mil order by [RowCount] desc
drop table if exists #CopyMonth

-- remove tables where the data has only one month in it

select	ROW_NUMBER() OVER (
			ORDER BY tbl.TableName
		   ) as RowNum,
		tbl.TableName,
		min(tbl.TableNoSchema) as TableNoSchema,
		tbl.ObjectID,
		tbl.[RowCount],
		min(sc.name) as MinColumnName,
		convert(bigint,0) as RowCountThisMonth,
		min(tbl.Done) as Done
--		min(sty.name) as DataTypeName
into	#CopyMonth
from	#Table10Mil tbl
left join	sys.columns sc on sc.object_id = tbl.ObjectID
left join	sys.types sty on sty.user_type_id = sc.user_type_id
where	sty.name like ''%date%''
	--and	sc.name not like ''%Created%''
	--and	sc.name not like ''%updated%''
	--and	sc.name not like ''%hour%''
	and (sc.name like ''%Occurred%''
		or sc.name like ''%PostDateTime%''
		)
group by tbl.TableName,
		tbl.ObjectID,
		tbl.[RowCount]
order by tbl.TableName

select	* from #CopyMonth

set nocount on
declare @DropOneMonthTable nvarchar(max)
declare @DropOriginalTable nvarchar(1000)
declare @DateBackMath nvarchar(300)
declare @InsertSql nvarchar(max)
declare @TableName nvarchar(200)
declare @IxDef nvarchar(max)
declare @TableDef nvarchar(max)
declare @TableDefOneMonth nvarchar(max)
declare @ColList nvarchar(max)
declare @RenameOneMonth nvarchar(1000)
declare @RowNum int = 0
declare @TableOneMonthCount nvarchar(max)
declare @TableOneMonthParams nvarchar(255) = ''@Count INT OUTPUT''
declare @TableOneMonthRowCount int = 0


/*
	1. drop table ''OneMonth'',							@DropOneMonthTable
	2. apply PK											@TableDefOneMonth
	3. insert into new ''OneMonth'' table from Original	@InsertSql
	4. drop Original table								@DropOriginalTable
	5. rename OneMonth									@RenameOneMonth
	6. apply PK											@TableDefOneMonth
	7. apply indices									@IxDef

*/
while (select count(*) from #CopyMonth where Done = 0) > 0
begin
	set @RowNum = @RowNum + 1

	-- SchemaName.TableName
	select	 @TableName = TableName
	from	#CopyMonth
	where	RowNum = @RowNum

	-- @ColList needed for when its an indentity table
		-- https://www.sqlshack.com/string_agg-function-in-sql/
	SELECT	@ColList = STRING_AGG(sc.name,'','') WITHIN GROUP (ORDER BY sc.column_id ASC)
	FROM sys.columns sc
	JOIN sys.tables st on st.object_id = sc.object_id
	JOIN sys.schemas ss on ss.schema_id = st.schema_id
	WHERE  ss.name + ''.'' + st.name = @TableName

	-- index definition
	set @IxDef = ''''
	EXEC [dev].IndexDefinition 
		@SchemaTableName = @TableName
		, @IndexDefs = @IxDef OUTPUT


	-- print DATEADD(month, DATEDIFF(month, 0, getdate()), 0) - first day of month
	if ( day(getdate()) < 9 )
		-- last month
		set @DateBackMath = ''DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0)''
	else
		-- this month
		set @DateBackMath = ''DATEADD(month, DATEDIFF(month, 0, getdate()), 0)''
	select	 @TableName = TableName
			,@DropOneMonthTable = ''DROP TABLE IF EXISTS '' +TableName+ ''OneMonth;''
			,@InsertSql = ''INSERT INTO '' +TableName+ ''OneMonth(''+@ColList+'') SELECT * FROM '' + TableName + '' WHERE '' + MinColumnName + '' > '' +@DateBackMath+ '';''
			,@DropOriginalTable = ''DROP TABLE IF EXISTS '' +TableName+ '';''
			-- remove schema from TableName
			,@RenameOneMonth = ''EXEC sp_rename @objname = '''''' +TableName+ ''OneMonth'''', @newname = '''''' +TableNoSchema+ '''''''' + '';''
			,@TableOneMonthCount = ''SELECT @Count = COUNT(*) FROM '' + @Tablename+ ''OneMonth''
	from	#CopyMonth
	where	RowNum = @RowNum

	print ''-- '' + @TableName

	-- check if has IDENTITY column
	IF (OBJECTPROPERTY(OBJECT_ID(@TableName), ''TableHasIdentity'') = 1) 
	BEGIN
		set @InsertSql = ''SET IDENTITY_INSERT ''+@TableName+''OneMonth ON;'' 
					+ @InsertSql 
					+ ''SET IDENTITY_INSERT ''+@TableName+''OneMonth OFF;''
	END
	-- SET IDENTITY_INSERT sometableWithIdentity ON/OFF


	-- table definition
	drop table if exists #TableDef
	create table #TableDef (
		SchemaName nvarchar(50),
		TableName nvarchar(200),
		Item varchar(max)
	)
	insert into #TableDef( 
		SchemaName
		, TableName
		, Item )
	exec dev.TableDefinition @TableName

	UPDATE	#TableDef
	SET		Item = Item + '';''

	SELECT	@TableDef = Item
			, @TableDefOneMonth = replace(Item, 
									''['' + SchemaName + ''].['' + TableName + '']'', 
									''['' + SchemaName + ''].['' + TableName + ''OneMonth]'')
	from	#TableDef

	
	print @DropOneMonthTable
	exec sp_executesql @DropOneMonthTable

	print @TableDefOneMonth
	exec sp_executesql @TableDefOneMonth

	print @InsertSql
	exec sp_executesql @InsertSql

	-- check table existence and row count more than 0
	if (	select	count(*) 
			from	sys.tables t
			join	sys.schemas s on s.schema_id = t.schema_id
			where	s.name + ''.'' + t.name = @TableName) = 0 
	begin
			print @TableName + '' was never created''
	end

	print @TableOneMonthCount
	EXEC sp_executeSQL @TableOneMonthCount, @TableOneMonthParams, @Count = @TableOneMonthRowCount OUTPUT;

	if (@TableOneMonthRowCount = 0)
	begin
		print @TableName + '' has no records before this month''
	end
	else
	begin
		print @DropOriginalTable
		exec sp_executesql @DropOriginalTable
	
		print @RenameOneMonth
		exec sp_executesql @RenameOneMonth

		print @IxDef
		exec sp_executesql @IxDef
	end

	print ''----------------------------------------------------------------''

	update	#CopyMonth
	set		Done = 1 
	where	RowNum = @RowNum

end	-- while (select count(*) from #CopyMonth where Done = 0)

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [dev.spMaskData]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'dev.spMaskData', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- 2 hours
exec [YourDatabaseDev].dev.spMaskData', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [YourDatabaseDevShrinkReIndex.sql]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'YourDatabaseDevShrinkReIndex.sql', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- YourDatabaseDevShrinkReIndex.sql
USE [YourDatabaseDev]
GO
-- shrink database (5.5 hours)
DBCC SHRINKDATABASE(N''YourDatabaseDev'' )
GO

-- reindex everything (1 hour)
-- Source:  https://ola.hallengren.com
EXECUTE master.[dbo].[IndexOptimize]
	@Databases = ''YourDatabaseDev'',
	@LogToTable = ''Y''', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [YourDatabaseDevBackup.sql]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'YourDatabaseDevBackup.sql', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- YourDatabaseDevBackup.sql
	-- 25 minutes
BACKUP DATABASE [YourDatabaseDev] 
	TO  DISK = N''c:\SQLBackup\YourDatabaseDev.bak'' 
	WITH NOFORMAT, INIT,  
	NAME = N''YourDatabaseDev-Full Database Backup'', 
	SKIP, NOREWIND, NOUNLOAD,  STATS = 10', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [DROP DATABASE [YourDatabaseDev]]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'DROP DATABASE [YourDatabaseDev]', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE [master]
GO
ALTER DATABASE [YourDatabaseDev] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
USE [master]
GO
/****** Object:  Database [YourDatabaseDev]    Script Date: 2/24/2022 3:08:46 PM ******/
DROP DATABASE [YourDatabaseDev]
GO
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Done]    Script Date: 10/28/2022 2:55:06 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Done', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'print getdate()', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Sunday night 10 pm', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20220211, 
		@active_end_date=99991231, 
		@active_start_time=220000, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


