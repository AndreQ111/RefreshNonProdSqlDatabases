drop procedure if exists dev.spMaskData
go

create procedure dev.spMaskData
as
/*
Example:
	exec spMaskData

Description:
	replace financial columns for environments that are not current production

	Code in SQL job "YourDatabaseDevRestore" on SandboxServer

	May also consider this, if implementing security roles
	https://cloudblogs.microsoft.com/sqlserver/2016/01/25/use-dynamic-data-masking-to-obfuscate-your-sensitive-data/
	but has a lot of vulnerabilities
	https://www.mssqltips.com/sqlservertip/4002/understand-the-limitations-of-sql-server-dynamic-data-masking/

History
	1/12/22		Andre Quitta	created
	1/13/22		Andre Quitta	five 1's interim update
								changed set RowCount to 1/500th of total rows
*/

begin
set nocount on
	drop table if exists #SchemaTableColumn

	create table #SchemaTableColumn (
		ID int identity(1,1),
		SchemaTableName varchar(200),
		ColumnName varchar(50),
		DataType varchar(20),
		IsFound bit,
		IsMasked bit
		)

	insert into #SchemaTableColumn (SchemaTableName, ColumnName)
	select 'dbo.HasFinancialInfo', 'ColumnToBeMasked'   union
	select 'dbo.HasFinancialInfo2', 'Column2ToBeMasked'   


	select	distinct
			OBJECT_SCHEMA_NAME(sc.object_id) as SchemaName,
			OBJECT_NAME(sc.object_id) as TableName,
			OBJECT_SCHEMA_NAME(sc.object_id) + '.' + OBJECT_NAME(sc.object_id) as SchemaTableName,
			sc.name as ColName, 
			st.name as DataTypeName
	from	sys.columns sc
	join	sys.types st 
		on	st.user_type_id = sc.user_type_id
	join	#SchemaTableColumn stc 
		on	stc.SchemaTableName  = OBJECT_SCHEMA_NAME(sc.object_id) + '.' + OBJECT_NAME(sc.object_id)
		and	stc.ColumnName = sc.name

	update	#SchemaTableColumn
	set		IsFound = 1,
			DataType = st.name
	from	sys.columns sc
	join	sys.types st 
		on	st.user_type_id = sc.user_type_id
	join	#SchemaTableColumn stc 
		on	stc.SchemaTableName  = OBJECT_SCHEMA_NAME(sc.object_id) + '.' + OBJECT_NAME(sc.object_id)
		and	stc.ColumnName = sc.name 


	select	*
	from	#SchemaTableColumn
	order by SchemaTableName, ColumnName


	-- prefix each new obfuscated number with '11111'
	declare @Prefix varchar(10) = QUOTENAME('11111', CHAR(39))
	declare @UpperLimit varchar(20) = QUOTENAME('1000', CHAR(39))
	declare @SchemaTableName varchar(200)
	declare @ColumnName varchar(50)
	declare @DataType varchar(10)
	declare @Sql nvarchar(max)
	declare @ID int = 1


	while (select count(*) from #SchemaTableColumn where IsMasked is null) > 0
	begin
		select	@SchemaTableName = SchemaTableName
				, @ColumnName = ColumnName
				, @DataType = DataType
		from	#SchemaTableColumn
		where	ID = @ID
print '-- (' + convert(varchar(5), @ID) + ')' + @SchemaTableName + '.' + @ColumnName 
print '-- ' + convert(varchar(50),getdate())

		set @Sql = '
			declare @RowCount int		-- how many rows to have the same random number, done for larger tables

			if (select count(*) from ' +@SchemaTableName+ ') < 500
				set @RowCount = 1
			else
				select @RowCount = coalesce(convert(int,count(*)/500),1) from ' +@SchemaTableName+ '

			while (select count(*) from ' +@SchemaTableName+ ' where left(convert(varchar(max), ' +@ColumnName+ '),len(' +@Prefix+ ')) <> ' +@Prefix+ ') > 0
			begin
				set rowcount @RowCount

				update	' +@SchemaTableName+ ' 
				set		' +@ColumnName+ ' = convert('+@DataType+',' +@Prefix+ ' + convert(varchar(50),ABS(CHECKSUM(NEWID())) % '+@UpperLimit+'))
				where	left(convert(varchar(max), ' +@ColumnName+ '),5) <> ' +@Prefix+ '

				set rowcount 0
			end

			update	' +@SchemaTableName+ '
			set		' +@ColumnName+ ' = convert('+@DataType+',
											right(convert(varchar(50),' +@ColumnName+ '), 
												len(' +@ColumnName+ ') - len(' +@Prefix+ ')
											)
										)

			'
		print @Sql
		exec sp_executesql @Sql

		update	#SchemaTableColumn
		set		IsMasked = 1
		where	ID = @ID

		set @ID = @ID + 1
	end	-- while (select count(*) from #SchemaTableColumn where IsMasked is null) > 0

end	-- create procedure dev.spMaskData
