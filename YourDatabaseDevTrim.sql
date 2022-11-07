-- YourDatabaseDevTrim.sql

/*
Description:
	delete records in the Data tables that are older than 1 month and have a date column that's named like 'occurred'
		done by copying 1 month's of data to a 2nd table and renaming it back to the original name
		including identity columns, PKs and indices


	Code in SQL job "YourDatabaseDevCreate" on SandboxServer
Dependency:
	Dev.IndexDefinition
	Dev.TableDefinition
History
	12/8/21		Andre Quitta	Created
	12/9/21		Andre Quitta	Dynamic table collection, deleting records
	12/10/21	Andre Quitta	Some tables are not indexed on date, looking to leverage IDs instead and custom queries
	1/7/22		Andre Quitta	sp_rename fixed, indices listed in separate routine
	2/15/22		Andre Quitta	exec sp_executesql @DropOneMonthTable, corrected comments
	3/1/22		Andre Quitta	updated to DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0) for 1st day previous month
	3/9/22		Andre Qutita	blanked out @IxDef in the beginning of WHILE statement
								deleting any existing table that has 'OneMonth' suffix
	3/24/22		Andre, Genessa	Adding column PostDateTime to 10 million record table WHERE

*/
USE [YourDatabaseDev]

-- ensure any previous 'OneMonth' tables are removed, allowing for safe repeated executions
DROP TABLE IF EXISTS #DeleteOneMonthTables

SELECT	s.name + '.' + t.name as TableName,
		0 as Done
INTO	#DeleteOneMonthTables
FROM	sys.tables t
JOIN	sys.schemas s on s.schema_id = t.schema_id
WHERE	t.name like '%OneMonth'

DECLARE @DeleteOneMonthSql NVARCHAR(2000)
DECLARE @OneMonthTable NVARCHAR(200)
WHILE (SELECT COUNT(*) FROM #DeleteOneMonthTables WHERE Done = 0)  > 0
BEGIN
	SELECT	@OneMonthTable = MIN(TableName)
	FROM	#DeleteOneMonthTables WHERE Done = 0

	SET @DeleteOneMonthSql = 'DROP TABLE ' + @OneMonthTable
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
SELECT	SCHEMA_NAME(sOBJ.schema_id) + '.' + sOBJ.name AS [TableName]
		, sOBJ.name as TableNoSchema
		, min(sOBJ.object_id) as ObjectID
		, SUM(sPTN.Rows) AS [RowCount]
		, 0 as Done
INTO	#Table10Mil
FROM	sys.objects AS sOBJ
INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id
WHERE	sOBJ.type = 'U'
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
where	sty.name like '%date%'
	--and	sc.name not like '%Created%'
	--and	sc.name not like '%updated%'
	--and	sc.name not like '%hour%'
group by tbl.TableName,
		tbl.ObjectID,
		tbl.[RowCount]
order by tbl.TableName

--select TableName, [RowCount] from #Table10Mil order by TableName
--select	TableName, RowCount, DateColumn = MinColumnName from #CopyMonth order by TableName

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
declare @TableOneMonthParams nvarchar(255) = '@Count INT OUTPUT'
declare @TableOneMonthRowCount int = 0


/*
	1. drop table 'OneMonth',							@DropOneMonthTable
	2. apply PK											@TableDefOneMonth
	3. insert into new 'OneMonth' table from Original	@InsertSql
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
	SELECT	@ColList = STRING_AGG(sc.name,',') WITHIN GROUP (ORDER BY sc.column_id ASC)
	FROM sys.columns sc
	JOIN sys.tables st on st.object_id = sc.object_id
	JOIN sys.schemas ss on ss.schema_id = st.schema_id
	WHERE  ss.name + '.' + st.name = @TableName

	-- index definition
	set @IxDef = ''
	EXEC [Audit].IndexDefinition 
		@SchemaTableName = @TableName
		, @IndexDefs = @IxDef OUTPUT


	-- print DATEADD(month, DATEDIFF(month, 0, getdate()), 0) - first day of month
	if ( day(getdate()) < 9 )
		-- last month
		set @DateBackMath = 'DATEADD(month, DATEDIFF(month, -1, getdate()) - 2, 0)'
	else
		-- this month
		set @DateBackMath = 'DATEADD(month, DATEDIFF(month, 0, getdate()), 0)'
	select	 @TableName = TableName
			,@DropOneMonthTable = 'DROP TABLE IF EXISTS ' +TableName+ 'OneMonth;'
			,@InsertSql = 'INSERT INTO ' +TableName+ 'OneMonth('+@ColList+') SELECT * FROM ' + TableName + ' WHERE ' + MinColumnName + ' > ' +@DateBackMath+ ';'
			,@DropOriginalTable = 'DROP TABLE IF EXISTS ' +TableName+ ';'
			-- remove schema from TableName
			,@RenameOneMonth = 'EXEC sp_rename @objname = ''' +TableName+ 'OneMonth'', @newname = ''' +TableNoSchema+ '''' + ';'
			,@TableOneMonthCount = 'SELECT @Count = COUNT(*) FROM ' + @Tablename+ 'OneMonth'
	from	#CopyMonth
	where	RowNum = @RowNum

	print '-- ' + @TableName

	-- check if has IDENTITY column
	IF (OBJECTPROPERTY(OBJECT_ID(@TableName), 'TableHasIdentity') = 1) 
	BEGIN
		set @InsertSql = 'SET IDENTITY_INSERT '+@TableName+'OneMonth ON;' 
					+ @InsertSql 
					+ 'SET IDENTITY_INSERT '+@TableName+'OneMonth OFF;'
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
	exec Audit.TableDefinition @TableName

	UPDATE	#TableDef
	SET		Item = Item + ';'

	SELECT	@TableDef = Item
			, @TableDefOneMonth = replace(Item, 
									'[' + SchemaName + '].[' + TableName + ']', 
									'[' + SchemaName + '].[' + TableName + 'OneMonth]')
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
			where	s.name + '.' + t.name = @TableName) = 0 
	begin
			print @TableName + ' was never created'
	end

	print @TableOneMonthCount
	EXEC sp_executeSQL @TableOneMonthCount, @TableOneMonthParams, @Count = @TableOneMonthRowCount OUTPUT;

	if (@TableOneMonthRowCount = 0)
	begin
		print @TableName + ' has no records before this month'
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

	print '----------------------------------------------------------------'

	update	#CopyMonth
	set		Done = 1 
	where	RowNum = @RowNum

end	-- while (select count(*) from #CopyMonth where Done = 0)

