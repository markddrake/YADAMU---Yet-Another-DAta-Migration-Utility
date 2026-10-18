use master
go
--
-- YADAMU_EXPORT_DDL - SQL Server DDL Export
--
-- Generates DDL statements for all objects in a given schema.
-- Equivalent to the Oracle YADAMU_EXPORT_DDL package.
--
-- Usage:
--   EXEC sp_YADAMU_EXPORT_DDL @P_SCHEMA = 'dbo'
--
drop procedure if exists sp_YADAMU_EXPORT_DDL
go
--
create or alter procedure sp_YADAMU_EXPORT_DDL(@P_SCHEMA NVARCHAR(128))
as
begin
  set nocount on;
  --
  declare @DDL_STATEMENTS table (
    ORDINAL       INT IDENTITY(1,1)
  , DDL_STATEMENT NVARCHAR(MAX)
  );
  --
  declare @DDL_STATEMENT    NVARCHAR(MAX);
  declare @TABLE_NAME       NVARCHAR(128);
  declare @OBJECT_NAME      NVARCHAR(128);
  declare @CONSTRAINT_NAME  NVARCHAR(128);
  declare @SCHEMA_ID        INT;
  --
  SET @SCHEMA_ID = SCHEMA_ID(@P_SCHEMA);
  --
  if @SCHEMA_ID is null begin
    RAISERROR('Schema "%s" does not exist', 16, 1, @P_SCHEMA);
    return;
  end;
  --
  -- =============================================
  -- 1. USER-DEFINED TYPES (table types, alias types, CLR types)
  -- =============================================
  --
  -- Alias types (scalar UDTs)
  --
  declare TYPE_CURSOR cursor local fast_forward for
    select t.name as TYPE_NAME,
           TYPE_NAME(t.system_type_id) as BASE_TYPE_NAME,
           t.max_length as MAX_LENGTH,
           t.precision as PRECISION,
           t.scale as SCALE,
           t.is_nullable as IS_NULLABLE,
           dc.definition as DEFAULT_DEF
      from sys.types t
      left join sys.default_constraints dc on dc.parent_object_id = 0 and dc.object_id = t.default_object_id
     where t.schema_id = @SCHEMA_ID
       and t.is_user_defined = 1
       and t.is_table_type = 0
       and t.is_assembly_type = 0
     order by t.name;
  --
  declare @TYPE_NAME      NVARCHAR(128);
  declare @BASE_TYPE_NAME NVARCHAR(128);
  declare @MAX_LENGTH     SMALLINT;
  declare @PRECISION      TINYINT;
  declare @SCALE          TINYINT;
  declare @IS_NULLABLE    BIT;
  declare @DEFAULT_DEF    NVARCHAR(MAX);
  --
  open TYPE_CURSOR;
  fetch next from TYPE_CURSOR into @TYPE_NAME, @BASE_TYPE_NAME, @MAX_LENGTH, @PRECISION, @SCALE, @IS_NULLABLE, @DEFAULT_DEF;
  while @@FETCH_STATUS = 0 begin
    SET @DDL_STATEMENT = 'CREATE TYPE ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TYPE_NAME) + ' FROM ' + @BASE_TYPE_NAME;
    --
    if @BASE_TYPE_NAME in ('varchar', 'nvarchar', 'char', 'nchar', 'varbinary', 'binary') begin
      if @MAX_LENGTH = -1
        SET @DDL_STATEMENT = @DDL_STATEMENT + '(max)';
      else begin
        if @BASE_TYPE_NAME in ('nvarchar', 'nchar')
          SET @DDL_STATEMENT = @DDL_STATEMENT + '(' + CAST(@MAX_LENGTH / 2 AS NVARCHAR(10)) + ')';
        else
          SET @DDL_STATEMENT = @DDL_STATEMENT + '(' + CAST(@MAX_LENGTH AS NVARCHAR(10)) + ')';
      end;
    end;
    --
    if @BASE_TYPE_NAME in ('decimal', 'numeric')
      SET @DDL_STATEMENT = @DDL_STATEMENT + '(' + CAST(@PRECISION AS NVARCHAR(10)) + ',' + CAST(@SCALE AS NVARCHAR(10)) + ')';
    --
    if @IS_NULLABLE = 1
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' NULL';
    else
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' NOT NULL';
    --
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    fetch next from TYPE_CURSOR into @TYPE_NAME, @BASE_TYPE_NAME, @MAX_LENGTH, @PRECISION, @SCALE, @IS_NULLABLE, @DEFAULT_DEF;
  end;
  close TYPE_CURSOR;
  deallocate TYPE_CURSOR;
  --
  -- Table types
  --
  declare TT_CURSOR cursor local fast_forward for
    select t.name as TYPE_NAME, tt.type_table_object_id
      from sys.table_types tt
      join sys.types t on t.user_type_id = tt.user_type_id
     where t.schema_id = @SCHEMA_ID
     order by t.name;
  --
  declare @TYPE_TABLE_OID INT;
  --
  open TT_CURSOR;
  fetch next from TT_CURSOR into @TYPE_NAME, @TYPE_TABLE_OID;
  while @@FETCH_STATUS = 0 begin
    --
    SET @DDL_STATEMENT = 'CREATE TYPE ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TYPE_NAME) + ' AS TABLE (' + CHAR(10);
    --
    select @DDL_STATEMENT = @DDL_STATEMENT + STRING_AGG(
             '  ' + QUOTENAME(c.name) + ' ' + TYPE_NAME(c.user_type_id)
             + case
                 when TYPE_NAME(c.user_type_id) in ('varchar','nvarchar','char','nchar','varbinary','binary') then
                   case when c.max_length = -1 then '(max)'
                        when TYPE_NAME(c.user_type_id) in ('nvarchar','nchar') then '(' + CAST(c.max_length/2 AS NVARCHAR(10)) + ')'
                        else '(' + CAST(c.max_length AS NVARCHAR(10)) + ')'
                   end
                 when TYPE_NAME(c.user_type_id) in ('decimal','numeric') then '(' + CAST(c.precision AS NVARCHAR(10)) + ',' + CAST(c.scale AS NVARCHAR(10)) + ')'
                 else ''
               end
             + case when c.is_nullable = 1 then ' NULL' else ' NOT NULL' end
           , ',' + CHAR(10))
      from sys.columns c
     where c.object_id = @TYPE_TABLE_OID
     order by c.column_id;
    --
    SET @DDL_STATEMENT = @DDL_STATEMENT + CHAR(10) + ')';
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    --
    fetch next from TT_CURSOR into @TYPE_NAME, @TYPE_TABLE_OID;
  end;
  close TT_CURSOR;
  deallocate TT_CURSOR;
  --
  -- =============================================
  -- 2. SEQUENCES
  -- =============================================
  --
  declare SEQ_CURSOR cursor local fast_forward for
    select s.name as SEQ_NAME,
           TYPE_NAME(s.system_type_id) as SEQ_TYPE,
           s.start_value,
           s.increment,
           s.minimum_value,
           s.maximum_value,
           s.is_cycling,
           s.cache_size,
           s.is_cached
      from sys.sequences s
     where s.schema_id = @SCHEMA_ID
     order by s.name;
  --
  declare @SEQ_NAME    NVARCHAR(128);
  declare @SEQ_TYPE    NVARCHAR(128);
  declare @START_VALUE SQL_VARIANT;
  declare @INCREMENT   SQL_VARIANT;
  declare @MIN_VALUE   SQL_VARIANT;
  declare @MAX_VALUE   SQL_VARIANT;
  declare @IS_CYCLING  BIT;
  declare @CACHE_SIZE  INT;
  declare @IS_CACHED   BIT;
  --
  open SEQ_CURSOR;
  fetch next from SEQ_CURSOR into @SEQ_NAME, @SEQ_TYPE, @START_VALUE, @INCREMENT, @MIN_VALUE, @MAX_VALUE, @IS_CYCLING, @CACHE_SIZE, @IS_CACHED;
  while @@FETCH_STATUS = 0 begin
    SET @DDL_STATEMENT = 'CREATE SEQUENCE ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@SEQ_NAME) + ' AS ' + @SEQ_TYPE
      + ' START WITH ' + CAST(@START_VALUE AS NVARCHAR(40))
      + ' INCREMENT BY ' + CAST(@INCREMENT AS NVARCHAR(40))
      + ' MINVALUE ' + CAST(@MIN_VALUE AS NVARCHAR(40))
      + ' MAXVALUE ' + CAST(@MAX_VALUE AS NVARCHAR(40));
    --
    if @IS_CACHED = 1
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' CACHE ' + CAST(@CACHE_SIZE AS NVARCHAR(10));
    else
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' NO CACHE';
    --
    if @IS_CYCLING = 1
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' CYCLE';
    else
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' NO CYCLE';
    --
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    fetch next from SEQ_CURSOR into @SEQ_NAME, @SEQ_TYPE, @START_VALUE, @INCREMENT, @MIN_VALUE, @MAX_VALUE, @IS_CYCLING, @CACHE_SIZE, @IS_CACHED;
  end;
  close SEQ_CURSOR;
  deallocate SEQ_CURSOR;
  --
  -- =============================================
  -- 3. TABLES (with inline PRIMARY KEY and UNIQUE constraints)
  -- =============================================
  --
  declare TABLE_CURSOR cursor local fast_forward for
    select t.name, t.object_id
      from sys.tables t
     where t.schema_id = @SCHEMA_ID
       and t.is_ms_shipped = 0
       and t.type = 'U'
     order by t.name;
  --
  declare @TABLE_OBJECT_ID INT;
  declare @COLUMN_DEFS     NVARCHAR(MAX);
  declare @PK_DEF          NVARCHAR(MAX);
  declare @UQ_DEFS         NVARCHAR(MAX);
  --
  open TABLE_CURSOR;
  fetch next from TABLE_CURSOR into @TABLE_NAME, @TABLE_OBJECT_ID;
  while @@FETCH_STATUS = 0 begin
    --
    -- Build column definitions
    --
    SET @COLUMN_DEFS = '';
    select @COLUMN_DEFS = STRING_AGG(
             '  ' + QUOTENAME(c.name) + ' '
             + case
                 when c.is_computed = 1 then 'AS ' + cc.definition + case when cc.is_persisted = 1 then ' PERSISTED' else '' end
                 else
                   TYPE_NAME(c.user_type_id)
                   + case
                       when TYPE_NAME(c.user_type_id) in ('varchar','nvarchar','char','nchar','varbinary','binary') then
                         case when c.max_length = -1 then '(max)'
                              when TYPE_NAME(c.user_type_id) in ('nvarchar','nchar') then '(' + CAST(c.max_length/2 AS NVARCHAR(10)) + ')'
                              else '(' + CAST(c.max_length AS NVARCHAR(10)) + ')'
                         end
                       when TYPE_NAME(c.user_type_id) in ('decimal','numeric') then '(' + CAST(c.precision AS NVARCHAR(10)) + ',' + CAST(c.scale AS NVARCHAR(10)) + ')'
                       when TYPE_NAME(c.user_type_id) in ('datetime2','datetimeoffset','time') then '(' + CAST(c.scale AS NVARCHAR(10)) + ')'
                       else ''
                     end
                   + case when c.is_identity = 1 then ' IDENTITY(' + CAST(IDENT_SEED(QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TABLE_NAME)) AS NVARCHAR(40)) + ',' + CAST(IDENT_INCR(QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TABLE_NAME)) AS NVARCHAR(40)) + ')' else '' end
                   + case when c.is_nullable = 1 then ' NULL' else ' NOT NULL' end
                   + case when dc.definition is not null then ' DEFAULT ' + dc.definition else '' end
               end
           , ',' + CHAR(10))
      from sys.columns c
      left join sys.default_constraints dc on dc.parent_object_id = c.object_id and dc.parent_column_id = c.column_id
      left join sys.computed_columns cc on cc.object_id = c.object_id and cc.column_id = c.column_id
     where c.object_id = @TABLE_OBJECT_ID
     order by c.column_id;
    --
    -- Inline PRIMARY KEY
    --
    SET @PK_DEF = NULL;
    select @PK_DEF = '  CONSTRAINT ' + QUOTENAME(kc.name) + ' PRIMARY KEY'
           + case when i.type = 1 then ' CLUSTERED' else ' NONCLUSTERED' end
           + ' (' + STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) + ')'
      from sys.key_constraints kc
      join sys.indexes i on i.object_id = kc.parent_object_id and i.index_id = kc.unique_index_id
      join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.is_included_column = 0
      join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
     where kc.parent_object_id = @TABLE_OBJECT_ID
       and kc.type = 'PK'
     group by kc.name, i.type;
    --
    if @PK_DEF is not null
      SET @COLUMN_DEFS = @COLUMN_DEFS + ',' + CHAR(10) + @PK_DEF;
    --
    -- Inline UNIQUE constraints
    --
    declare UQ_CURSOR cursor local fast_forward for
      select kc.name as CONSTRAINT_NAME,
             case when i.type = 1 then ' CLUSTERED' else ' NONCLUSTERED' end as INDEX_TYPE,
             STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as UQ_COLUMNS
        from sys.key_constraints kc
        join sys.indexes i on i.object_id = kc.parent_object_id and i.index_id = kc.unique_index_id
        join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.is_included_column = 0
        join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
       where kc.parent_object_id = @TABLE_OBJECT_ID
         and kc.type = 'UQ'
       group by kc.name, i.type
       order by kc.name;
    --
    declare @INDEX_TYPE NVARCHAR(20);
    declare @UQ_COLUMNS NVARCHAR(MAX);
    --
    open UQ_CURSOR;
    fetch next from UQ_CURSOR into @CONSTRAINT_NAME, @INDEX_TYPE, @UQ_COLUMNS;
    while @@FETCH_STATUS = 0 begin
      SET @COLUMN_DEFS = @COLUMN_DEFS + ',' + CHAR(10) + '  CONSTRAINT ' + QUOTENAME(@CONSTRAINT_NAME) + ' UNIQUE' + @INDEX_TYPE + ' (' + @UQ_COLUMNS + ')';
      fetch next from UQ_CURSOR into @CONSTRAINT_NAME, @INDEX_TYPE, @UQ_COLUMNS;
    end;
    close UQ_CURSOR;
    deallocate UQ_CURSOR;
    --
    SET @DDL_STATEMENT = 'CREATE TABLE ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TABLE_NAME) + ' (' + CHAR(10) + @COLUMN_DEFS + CHAR(10) + ')';
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    --
    fetch next from TABLE_CURSOR into @TABLE_NAME, @TABLE_OBJECT_ID;
  end;
  close TABLE_CURSOR;
  deallocate TABLE_CURSOR;
  --
  -- =============================================
  -- 4. INDEXES (non-constraint indexes)
  -- =============================================
  --
  declare IDX_CURSOR cursor local fast_forward for
    select t.name as TABLE_NAME,
           i.name as INDEX_NAME,
           i.is_unique,
           i.type_desc as INDEX_TYPE_DESC,
           i.type as INDEX_TYPE,
           i.filter_definition,
           STRING_AGG(case when ic.is_included_column = 0 then QUOTENAME(c.name) end, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as KEY_COLUMNS,
           STRING_AGG(case when ic.is_included_column = 1 then QUOTENAME(c.name) end, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as INCLUDE_COLUMNS
      from sys.indexes i
      join sys.tables t on t.object_id = i.object_id
      join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
      join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
     where t.schema_id = @SCHEMA_ID
       and i.is_primary_key = 0
       and i.is_unique_constraint = 0
       and i.type > 0
       and i.name is not null
       and t.is_ms_shipped = 0
     group by t.name, i.name, i.is_unique, i.type_desc, i.type, i.filter_definition
     order by t.name, i.name;
  --
  declare @IS_UNIQUE        BIT;
  declare @INDEX_TYPE_DESC   NVARCHAR(60);
  declare @IDX_TYPE          INT;
  declare @FILTER_DEF        NVARCHAR(MAX);
  declare @KEY_COLUMNS       NVARCHAR(MAX);
  declare @INCLUDE_COLUMNS   NVARCHAR(MAX);
  declare @INDEX_NAME        NVARCHAR(128);
  --
  open IDX_CURSOR;
  fetch next from IDX_CURSOR into @TABLE_NAME, @INDEX_NAME, @IS_UNIQUE, @INDEX_TYPE_DESC, @IDX_TYPE, @FILTER_DEF, @KEY_COLUMNS, @INCLUDE_COLUMNS;
  while @@FETCH_STATUS = 0 begin
    SET @DDL_STATEMENT = 'CREATE ';
    if @IS_UNIQUE = 1 SET @DDL_STATEMENT = @DDL_STATEMENT + 'UNIQUE ';
    if @IDX_TYPE = 1
      SET @DDL_STATEMENT = @DDL_STATEMENT + 'CLUSTERED ';
    else if @IDX_TYPE = 2
      SET @DDL_STATEMENT = @DDL_STATEMENT + 'NONCLUSTERED ';
    --
    SET @DDL_STATEMENT = @DDL_STATEMENT + 'INDEX ' + QUOTENAME(@INDEX_NAME) + ' ON ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TABLE_NAME) + ' (' + @KEY_COLUMNS + ')';
    --
    if @INCLUDE_COLUMNS is not null
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' INCLUDE (' + @INCLUDE_COLUMNS + ')';
    --
    if @FILTER_DEF is not null
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' WHERE ' + @FILTER_DEF;
    --
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    fetch next from IDX_CURSOR into @TABLE_NAME, @INDEX_NAME, @IS_UNIQUE, @INDEX_TYPE_DESC, @IDX_TYPE, @FILTER_DEF, @KEY_COLUMNS, @INCLUDE_COLUMNS;
  end;
  close IDX_CURSOR;
  deallocate IDX_CURSOR;
  --
  -- =============================================
  -- 5. CHECK CONSTRAINTS (ALTER TABLE ADD CONSTRAINT)
  -- =============================================
  --
  declare CHK_CURSOR cursor local fast_forward for
    select t.name as TABLE_NAME,
           cc.name as CONSTRAINT_NAME,
           cc.definition
      from sys.check_constraints cc
      join sys.tables t on t.object_id = cc.parent_object_id
     where t.schema_id = @SCHEMA_ID
       and t.is_ms_shipped = 0
     order by t.name, cc.name;
  --
  declare @CHK_DEF NVARCHAR(MAX);
  --
  open CHK_CURSOR;
  fetch next from CHK_CURSOR into @TABLE_NAME, @CONSTRAINT_NAME, @CHK_DEF;
  while @@FETCH_STATUS = 0 begin
    SET @DDL_STATEMENT = 'ALTER TABLE ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TABLE_NAME) + ' ADD CONSTRAINT ' + QUOTENAME(@CONSTRAINT_NAME) + ' CHECK ' + @CHK_DEF;
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    fetch next from CHK_CURSOR into @TABLE_NAME, @CONSTRAINT_NAME, @CHK_DEF;
  end;
  close CHK_CURSOR;
  deallocate CHK_CURSOR;
  --
  -- =============================================
  -- 6. FOREIGN KEYS (ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY)
  -- =============================================
  --
  declare FK_CURSOR cursor local fast_forward for
    select fk.name as FK_NAME,
           OBJECT_NAME(fk.parent_object_id) as TABLE_NAME,
           STRING_AGG(QUOTENAME(cp.name), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) as FK_COLUMNS,
           QUOTENAME(SCHEMA_NAME(rt.schema_id)) + '.' + QUOTENAME(rt.name) as REF_TABLE,
           STRING_AGG(QUOTENAME(cr.name), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) as REF_COLUMNS,
           fk.delete_referential_action_desc as DELETE_ACTION,
           fk.update_referential_action_desc as UPDATE_ACTION
      from sys.foreign_keys fk
      join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
      join sys.columns cp on cp.object_id = fkc.parent_object_id and cp.column_id = fkc.parent_column_id
      join sys.columns cr on cr.object_id = fkc.referenced_object_id and cr.column_id = fkc.referenced_column_id
      join sys.tables rt on rt.object_id = fk.referenced_object_id
      join sys.tables pt on pt.object_id = fk.parent_object_id
     where pt.schema_id = @SCHEMA_ID
       and pt.is_ms_shipped = 0
     group by fk.name, fk.parent_object_id, rt.schema_id, rt.name, fk.delete_referential_action_desc, fk.update_referential_action_desc
     order by OBJECT_NAME(fk.parent_object_id), fk.name;
  --
  declare @FK_NAME      NVARCHAR(128);
  declare @FK_COLUMNS   NVARCHAR(MAX);
  declare @REF_TABLE    NVARCHAR(MAX);
  declare @REF_COLUMNS  NVARCHAR(MAX);
  declare @DELETE_ACTION NVARCHAR(60);
  declare @UPDATE_ACTION NVARCHAR(60);
  --
  open FK_CURSOR;
  fetch next from FK_CURSOR into @FK_NAME, @TABLE_NAME, @FK_COLUMNS, @REF_TABLE, @REF_COLUMNS, @DELETE_ACTION, @UPDATE_ACTION;
  while @@FETCH_STATUS = 0 begin
    SET @DDL_STATEMENT = 'ALTER TABLE ' + QUOTENAME(@P_SCHEMA) + '.' + QUOTENAME(@TABLE_NAME)
      + ' ADD CONSTRAINT ' + QUOTENAME(@FK_NAME)
      + ' FOREIGN KEY (' + @FK_COLUMNS + ')'
      + ' REFERENCES ' + @REF_TABLE + ' (' + @REF_COLUMNS + ')';
    --
    if @DELETE_ACTION <> 'NO_ACTION'
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' ON DELETE ' + REPLACE(@DELETE_ACTION, '_', ' ');
    if @UPDATE_ACTION <> 'NO_ACTION'
      SET @DDL_STATEMENT = @DDL_STATEMENT + ' ON UPDATE ' + REPLACE(@UPDATE_ACTION, '_', ' ');
    --
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@DDL_STATEMENT);
    fetch next from FK_CURSOR into @FK_NAME, @TABLE_NAME, @FK_COLUMNS, @REF_TABLE, @REF_COLUMNS, @DELETE_ACTION, @UPDATE_ACTION;
  end;
  close FK_CURSOR;
  deallocate FK_CURSOR;
  --
  -- =============================================
  -- 7. VIEWS
  -- =============================================
  --
  declare VIEW_CURSOR cursor local fast_forward for
    select v.name as VIEW_NAME,
           sm.definition as VIEW_DEF
      from sys.views v
      join sys.sql_modules sm on sm.object_id = v.object_id
     where v.schema_id = @SCHEMA_ID
       and v.is_ms_shipped = 0
     order by v.name;
  --
  declare @VIEW_DEF NVARCHAR(MAX);
  --
  open VIEW_CURSOR;
  fetch next from VIEW_CURSOR into @OBJECT_NAME, @VIEW_DEF;
  while @@FETCH_STATUS = 0 begin
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@VIEW_DEF);
    fetch next from VIEW_CURSOR into @OBJECT_NAME, @VIEW_DEF;
  end;
  close VIEW_CURSOR;
  deallocate VIEW_CURSOR;
  --
  -- =============================================
  -- 8. FUNCTIONS
  -- =============================================
  --
  declare FUNC_CURSOR cursor local fast_forward for
    select o.name as FUNC_NAME,
           sm.definition as FUNC_DEF
      from sys.objects o
      join sys.sql_modules sm on sm.object_id = o.object_id
     where o.schema_id = @SCHEMA_ID
       and o.type in ('FN', 'IF', 'TF')
       and o.is_ms_shipped = 0
     order by o.name;
  --
  declare @FUNC_DEF NVARCHAR(MAX);
  --
  open FUNC_CURSOR;
  fetch next from FUNC_CURSOR into @OBJECT_NAME, @FUNC_DEF;
  while @@FETCH_STATUS = 0 begin
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@FUNC_DEF);
    fetch next from FUNC_CURSOR into @OBJECT_NAME, @FUNC_DEF;
  end;
  close FUNC_CURSOR;
  deallocate FUNC_CURSOR;
  --
  -- =============================================
  -- 9. PROCEDURES
  -- =============================================
  --
  declare PROC_CURSOR cursor local fast_forward for
    select p.name as PROC_NAME,
           sm.definition as PROC_DEF
      from sys.procedures p
      join sys.sql_modules sm on sm.object_id = p.object_id
     where p.schema_id = @SCHEMA_ID
       and p.is_ms_shipped = 0
     order by p.name;
  --
  declare @PROC_DEF NVARCHAR(MAX);
  --
  open PROC_CURSOR;
  fetch next from PROC_CURSOR into @OBJECT_NAME, @PROC_DEF;
  while @@FETCH_STATUS = 0 begin
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@PROC_DEF);
    fetch next from PROC_CURSOR into @OBJECT_NAME, @PROC_DEF;
  end;
  close PROC_CURSOR;
  deallocate PROC_CURSOR;
  --
  -- =============================================
  -- 10. TRIGGERS
  -- =============================================
  --
  declare TRIG_CURSOR cursor local fast_forward for
    select tr.name as TRIG_NAME,
           sm.definition as TRIG_DEF
      from sys.triggers tr
      join sys.sql_modules sm on sm.object_id = tr.object_id
      join sys.tables t on t.object_id = tr.parent_id
     where t.schema_id = @SCHEMA_ID
       and t.is_ms_shipped = 0
     order by t.name, tr.name;
  --
  declare @TRIG_DEF NVARCHAR(MAX);
  --
  open TRIG_CURSOR;
  fetch next from TRIG_CURSOR into @OBJECT_NAME, @TRIG_DEF;
  while @@FETCH_STATUS = 0 begin
    insert into @DDL_STATEMENTS (DDL_STATEMENT) values (@TRIG_DEF);
    fetch next from TRIG_CURSOR into @OBJECT_NAME, @TRIG_DEF;
  end;
  close TRIG_CURSOR;
  deallocate TRIG_CURSOR;
  --
  -- Return the result set
  --
  select DDL_STATEMENT
    from @DDL_STATEMENTS
   order by ORDINAL;
  --
end
--
go
EXECUTE sp_ms_marksystemobject 'sp_YADAMU_EXPORT_DDL'
go
