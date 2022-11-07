-- SqlJob_YourDatabaseDevRestore.sql
USE [msdb]
GO

/****** Object:  Job [YourDatabaseDevRestore] ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 10/28/2022 2:59:24 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'YourDatabaseDevRestore', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Restore YourDatabaseDev
based off of YourDatabaseDev backup
(15 minutes)', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'SERVICES\YourServiceAccount', 
		@notify_email_operator_name=N'SqlAlert', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [RESTORE DATABASE [YourDatabaseDev]]    Script Date: 10/28/2022 2:59:24 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'RESTORE DATABASE [YourDatabaseDev]', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- https://stackoverflow.com/questions/1154200/when-restoring-a-backup-how-do-i-disconnect-all-active-connections
ALTER DATABASE [YourDatabaseDev]
SET SINGLE_USER WITH
ROLLBACK AFTER 20 --this will give your current connections 20 seconds to complete

RESTORE DATABASE [YourDatabaseDev] 
	FROM  DISK = N''\SqlBackup$\YourDatabaseDev.bak'' 
	WITH  FILE = 1,  
	MOVE N''YourDatabaseDev'' TO N''c:\SQLData\YourDatabaseDev.mdf'',  
	MOVE N''YourDatabaseDev_log'' TO N''c:\SQLLogs\YourDatabaseDev_log.ldf'',  
	NOUNLOAD,  REPLACE,  STATS = 5

ALTER DATABASE [YourDatabaseDev] SET MULTI_USER
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


