-- Dev.IndexDefinition.sql
drop procedure if exists Dev.IndexDefinition
go




create procedure Dev.IndexDefinition 
	@SchemaTableName nvarchar(200), 
	@IndexDefs varchar(max) OUTPUT
as
/*
EXECUTE 

Generate and return the index definitions for a given table
	if no index is supplied, will return definition of all indexes
Copied from https://www.mssqltips.com/sqlservertip/3441/script-out-all-sql-server-indexes-in-a-database-using-tsql/
Used for reducing table sizes when copying from Production to Development, where 
	- 1 month's worth of data is copied over to a 2nd table
	- the original table is deleted
	- 2nd table is renamed the original table
	- indexes are reapplied		<-- this procedure

Example:
	declare @MyIndexDefs varchar(max)
	EXEC Dev.IndexDefinition 
		@SchemaTableName = 'Schema.TableName'
		, @IndexDefs = @MyIndexDefs OUTPUT
	print @MyIndexDefs


Date		Who				What
12/29/21	Andre Quitta	Created
12/30/21	Andre Quitta	Adjusted to return multiple indices
1/3/22		Andre Quitta	combined schema and table parameters
3/1/22		Andre Quitta	removed Fill_factor
*/
begin

	declare @SchemaName varchar(100)
	declare @TableName varchar(256)
	declare @IndexName varchar(256)
	declare @ColumnName varchar(100)
	declare @is_unique varchar(100)
	declare @IndexTypeDesc varchar(100)
	declare @FileGroupName varchar(100)
	declare @is_disabled varchar(100)
	declare @IndexOptions varchar(max)
	declare @IndexColumnId int
	declare @IsDescendingKey int 
	declare @IsIncludedColumn int
	declare @TSQLScripCreationIndex varchar(max)
	declare @TSQLScripDisableIndex varchar(max)
	declare @CrLf char(1) = char(13)

	declare CursorIndex cursor for
		 select schema_name(t.schema_id) [schema_name], t.name, ix.name
				, case when ix.is_unique = 1 then 'UNIQUE ' else '' END 
				, ix.type_desc
				, case when ix.is_padded=1 then 'PAD_INDEX = ON, ' else 'PAD_INDEX = OFF, ' end
					 + case when ix.allow_page_locks=1 then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
					 + case when ix.allow_row_locks=1 then  'ALLOW_ROW_LOCKS = ON, ' else 'ALLOW_ROW_LOCKS = OFF, ' end
					 + case when INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 then 'STATISTICS_NORECOMPUTE = ON, ' else 'STATISTICS_NORECOMPUTE = OFF, ' end
					 + case when ix.ignore_dup_key=1 then 'IGNORE_DUP_KEY = ON, ' else 'IGNORE_DUP_KEY = OFF, ' end
					 + 'SORT_IN_TEMPDB = OFF' AS IndexOptions
				, ix.is_disabled , FILEGROUP_NAME(ix.data_space_id) FileGroupName
		 from	sys.tables t 
		 inner join sys.indexes ix on t.object_id=ix.object_id
		 where	ix.type > 0 
			and ix.is_primary_key = 0 
			and ix.is_unique_constraint = 0 
			and schema_name(t.schema_id) + '.' + t.name = @SchemaTableName
			and t.is_ms_shipped=0 
			and t.name <> 'sysdiagrams'
		 order by schema_name(t.schema_id)
			, t.name
			, ix.name

	open CursorIndex
	fetch next 
	from	CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName

	declare @IndexColumns varchar(max)
	declare @IncludedColumns varchar(max)

	while (@@fetch_status=0)	-- open CursorIndex
	begin
 
		set @IndexColumns=''
		set @IncludedColumns=''
 
		declare CursorIndexColumn cursor for 
			select	col.name
					, ixc.is_descending_key
					, ixc.is_included_column
			from	sys.tables tb 
			inner join sys.indexes ix on tb.object_id=ix.object_id
			inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
			inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
			where	ix.type > 0 
				and (ix.is_primary_key=0 or ix.is_unique_constraint=0)
				and schema_name(tb.schema_id) = @SchemaName 
				and tb.name = @TableName 
				and ix.name=@IndexName
			order by ixc.index_column_id
 
		open CursorIndexColumn 
		fetch next 
		from CursorIndexColumn into  @ColumnName, @IsDescendingKey, @IsIncludedColumn
 
		while (@@fetch_status=0)	-- open CursorIndexColumn 
		begin
			if @IsIncludedColumn=0 
				set @IndexColumns=@IndexColumns + @ColumnName  + case when @IsDescendingKey=1  then ' DESC, ' else  ' ASC, ' end
			else 
				set @IncludedColumns=@IncludedColumns  + @ColumnName  +', ' 

			fetch next 
			from CursorIndexColumn into @ColumnName, @IsDescendingKey, @IsIncludedColumn
		end	-- while (@@fetch_status=0)	-- open CursorIndexColumn 

		close CursorIndexColumn
		deallocate CursorIndexColumn

		set @IndexColumns = substring(@IndexColumns, 1, len(@IndexColumns)-1)
		set @IncludedColumns = case when len(@IncludedColumns) >0 then substring(@IncludedColumns, 1, len(@IncludedColumns)-1) else '' end
		--  print @IndexColumns
		--  print @IncludedColumns

		set @TSQLScripCreationIndex =''
		set @TSQLScripDisableIndex =''
		set @TSQLScripCreationIndex='CREATE '+ @is_unique  +@IndexTypeDesc + ' INDEX ' +QUOTENAME(@IndexName)+' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName)+ '('+@IndexColumns+') '
			+ case when len(@IncludedColumns)>0 then @CrLf +'INCLUDE (' + @IncludedColumns+ ')' else '' end + @CrLf+'WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  
		set @IndexDefs = coalesce(@IndexDefs,' ') + @CrLf + @CrLf + @TSQLScripCreationIndex

		if @is_disabled=1 
			set  @TSQLScripDisableIndex=  @CrLf +'ALTER INDEX ' +QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName) + ' DISABLE;' + @CrLf 

		--print @TSQLScripCreationIndex
		--print @TSQLScripDisableIndex
		--print @IndexDefs
		--print '-----------'

		fetch next 
		from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName

	end	-- while (@@fetch_status=0)	-- open CursorIndex

	close CursorIndex
	deallocate CursorIndex


	
end	-- create procedure Dev.IndexDefinition 
