CREATE OR ALTER FUNCTION MAP_FOREIGN_DATATYPE(@SOURCE_VENDOR NVARCHAR(128), @DATA_TYPE NVARCHAR(128), @DATA_TYPE_LENGTH INT, @DATA_TYPE_SCALE INT) 
RETURNS VARCHAR(128) 
AS
BEGIN
  RETURN 
  case
    when @SOURCE_VENDOR = 'Oracle'
      then case 
             when @DATA_TYPE = 'VARCHAR2' 
               then 'varchar'
             when @DATA_TYPE = 'NVARCHAR2' 
               then 'nvarchar'
             when @DATA_TYPE = 'CLOB'
               then 'varchar(max)'
             when @DATA_TYPE = 'NCLOB'
               then 'nvarchar(max)'
             when @DATA_TYPE = 'NUMBER'
               then 'decimal'
             when @DATA_TYPE = 'BINARY_DOUBLE'
               then 'float'
             when @DATA_TYPE = 'BINARY_FLOAT'
               then 'real'
             when @DATA_TYPE = 'RAW'
               then 'varbinary'
             when @DATA_TYPE = 'BLOB'
               then 'varbinary(max)'
             when @DATA_TYPE = 'BFILE'
               then 'varchar(2048)'  
             when @DATA_TYPE in ('ROWID','UROWID') 
               then 'varchar(18)'                  
             when @DATA_TYPE in ('ANYDATA') 
               then 'nvarchar(max)'                
             when (CHARINDEX('INTERVAL',@DATA_TYPE) = 1)
               then 'varchar(64)'                  
             when (CHARINDEX('TIMESTAMP',@DATA_TYPE) = 1) 
               then case
                      when (CHARINDEX('TIME ZONE',@DATA_TYPE) > 0) 
                        then 'datetimeoffset'
                      else 
                       'datetime2' 
               end
             when @DATA_TYPE = 'XMLTYPE'
               then 'xml'
             when @DATA_TYPE like '"%"."%"'
               then 'nvarchar(max)'
             when @DATA_TYPE = 'JSON' 
               then 'nvarchar(max)'
             else
               lower(@DATA_TYPE)
           end
    when @SOURCE_VENDOR in ('MySQL','MariaDB')   
      then case 
             when @DATA_TYPE = 'mediumint' 
               then 'int'
             when @DATA_TYPE = 'timestamp' 
               then 'datetime'
             when @DATA_TYPE = 'enum'
               then 'varchar(255)'
             when @DATA_TYPE = 'set'
               then 'varchar(255)'
             when @DATA_TYPE = 'year'
               then 'smallint'
             when @DATA_TYPE = 'json' 
               then 'nvarchar(max)'
             when @DATA_TYPE = 'blob' and @DATA_TYPE_LENGTH > 8000  
               then 'varbinary(max)'
             when @DATA_TYPE = 'blob' 
               then 'varbinary'
             else
               lower(@DATA_TYPE)
           end
    else 
      lower(@DATA_TYPE)
  end
end
--
GO
--
CREATE OR ALTER FUNCTION GENERATE_STATEMENTS(@SOURCE_VENDOR NVARCHAR(128), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_LIST NVARCHAR(MAX),@DATA_TYPE_LIST NVARCHAR(MAX),@DATA_SIZE_LIST NVARCHAR(MAX)) 
RETURNS NVARCHAR(MAX)
AS
BEGIN
  DECLARE @COLUMNS_CLAUSE     NVARCHAR(MAX);
  DECLARE @INSERT_SELECT_LIST NVARCHAR(MAX);
  DECLARE @WITH_CLAUSE        NVARCHAR(MAX);
  DECLARE @TARGET_DATA_TYPES  NVARCHAR(MAX);
  
  DECLARE @DDL_STATEMENT      NVARCHAR(MAX);
  DECLARE @DML_STATEMENT      NVARCHAR(MAX);
 
  WITH "SOURCE_TABLE_DEFINITION" as (
          SELECT c."KEY" "INDEX"
                ,c."VALUE" "COLUMN_NAME"
                ,t."VALUE" "DATA_TYPE"
                ,case
                   when s.VALUE = ''
                     then NULL
                   when CHARINDEX(',',s."VALUE") > 0 
                     then LEFT(s."VALUE",CHARINDEX(',',s."VALUE")-1)
                   else
                     s."VALUE"
                 end "DATA_TYPE_LENGTH"
                ,case
                   when CHARINDEX(',',s."VALUE") > 0 
                     then RIGHT(s."VALUE", CHARINDEX(',',REVERSE(s."VALUE"))-1)
                   else 
                     NULL
                 end "DATA_TYPE_SCALE"
            FROM OPENJSON(CONCAT('[',@COLUMN_LIST,']')) c,
                 OPENJSON(@DATA_TYPE_LIST) t,
                 OPENJSON(@DATA_SIZE_LIST) s
           WHERE c."KEY" = t."KEY" and c."KEY" = s."KEY"
  ),
  "TARGET_TABLE_DEFINITION" as (
    select st.*, dbo.MAP_FOREIGN_DATATYPE(@SOURCE_VENDOR, "DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
      from "SOURCE_TABLE_DEFINITION" st
  )
  SELECT @COLUMNS_CLAUSE =
         STRING_AGG(CONCAT('"',"COLUMN_NAME",'" ',
                           case
                             when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0)  
                               then "TARGET_DATA_TYPE"
                             when "TARGET_DATA_TYPE" in('xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry','hierarchyid','uniqueidentifier')
                               then "TARGET_DATA_TYPE"                
                             when "DATA_TYPE_SCALE" IS NOT NULL
                               then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                             when "DATA_TYPE_LENGTH"  IS NOT NULL 
                               then case 
                                      when "DATA_TYPE_LENGTH" = -1 
                                        then CONCAT("TARGET_DATA_TYPE",'(max)')
                                        else CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                                    end
                             else 
                               "TARGET_DATA_TYPE"
                           end
                         ) 
                   ,','                 
                   )
        ,@TARGET_DATA_TYPES =
         CONCAT('[',
                STRING_AGG(CONCAT(
                                  '"',
                                  case
                                    when TARGET_DATA_TYPE LIKE '%(%}%'
                                      then "TARGET_DATA_TYPE"
                                    when "TARGET_DATA_TYPE" in('xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney')
                                      then "TARGET_DATA_TYPE"                 
                                    when "DATA_TYPE_SCALE" IS NOT NULL
                                      then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                                    when "DATA_TYPE_LENGTH"  IS NOT NULL 
                                      then case 
                                             when "DATA_TYPE_LENGTH" = -1 
                                               then CONCAT("TARGET_DATA_TYPE",'(max)')
                                             else 
                                               CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                                           end
                                    else 
                                      "TARGET_DATA_TYPE"
                                  end,
                                  '"'
                                 )
                           ,','                 
                          )
                ,']'
               )
        ,@INSERT_SELECT_LIST = 
         STRING_AGG(case
                      when "TARGET_DATA_TYPE" in ('binary','varbinary')
                        then case
                                when ((DATA_TYPE_LENGTH = -1) OR (DATA_TYPE = 'BLOB') or ((CHARINDEX('"."',DATA_TYPE) > 0)))
                                  then CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(max),"',"COLUMN_NAME",'") "',COLUMN_NAME,'"')
                                  else CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",'),"',"COLUMN_NAME",'") "',COLUMN_NAME,'"')
                             end
                      when "TARGET_DATA_TYPE" = 'image'
                        then CONCAT('convert(varchar(max),CONVERT(varbinary(max),"',"COLUMN_NAME",'"),2)')
                      else
                        CONCAT('"',"COLUMN_NAME",'"')
                    end 
                   ,','                 
                   )
       ,@WITH_CLAUSE =
        STRING_AGG(CONCAT('"',COLUMN_NAME,'" ',
                          case            
                            when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0)  
                              then "TARGET_DATA_TYPE"
                            when "TARGET_DATA_TYPE" in('xml','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney')
                              then "TARGET_DATA_TYPE"         
                            when "TARGET_DATA_TYPE" = 'image'
                              then 'nvarchar(max)'                
                            when "TARGET_DATA_TYPE" = 'text'
                              then 'varchar(max)'                 
                            when "TARGET_DATA_TYPE" = 'ntext'
                              then 'nvarchar(max)'                
                            when "TARGET_DATA_TYPE" in ('varchar','nvarchar') 
                              then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                            when "TARGET_DATA_TYPE" in ('binary','varbinary') 
                              then CONCAT('varchar(',cast(("DATA_TYPE_LENGTH" * 2) as VARCHAR),')')
                            when "DATA_TYPE_SCALE" IS NOT NULL
                              then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                            when "DATA_TYPE_LENGTH" IS NOT NULL 
                              then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                            else 
                              "TARGET_DATA_TYPE"
                          end,
                          ' ''$[',"INDEX",']'''
                        )
                        ,','                    
                   )
      FROM "TARGET_TABLE_DEFINITION" tt;
     
   SET @DDL_STATEMENT = CONCAT('if object_id(''"',@SCHEMA,'"."',@TABLE_NAME,'"'',''U'') is NULL create table "',@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMNS_CLAUSE,')');
   SET @DML_STATEMENT = CONCAT('insert into "' ,@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMN_LIST,') select ',@INSERT_SELECT_LIST,'  from "JSON_STAGING" cross apply OPENJSON("DATA",''$.data."',@TABLE_NAME,'"'') with ( ',@WITH_CLAUSE,') data');
   RETURN JSON_MODIFY(JSON_MODIFY(JSON_MODIFY('{}','$.ddl',@DDL_STATEMENT),'$.dml',@DML_STATEMENT),'$.targetDataTypes',JSON_QUERY(@TARGET_DATA_TYPES))
end;
GO
--
CREATE OR ALTER PROCEDURE IMPORT_JSON(@TARGET_DATABASE VARCHAR(128)) 
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @STATEMENTS       NVARCHAR(MAX);
  DECLARE @SQL_STATEMENT    NVARCHAR(MAX);
  
  DECLARE @START_TIME       DATETIME2;
  DECLARE @end_TIME         DATETIME2;
  DECLARE @ELAPSED_TIME     BIGINT;  
  DECLARE @ROW_COUNT        BIGINT;
 
  DECLARE @LOG_ENTRY        NVARCHAR(MAX);
  DECLARE @RESULTS          NVARCHAR(MAX) = '[]';
                            
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select TABLE_NAME, 
         dbo.GENERATE_STATEMENTS(SOURCE_VENDOR, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_LIST, v.DATA_TYPE_LIST, v.SIZE_CONSTRAINTS) as STATEMENTS
   from "JSON_STAGING"
         cross apply OPENJSON("DATA") 
         with (
           SOURCE_VENDOR nvarchar(128) '$.systemInformation.vendor'
          ,METADATA      nvarchar(max) '$.metadata' as json
         ) x
         cross apply OPENJSON(X.METADATA) y
		 cross apply OPENJSON(y.VALUE) 
		             with (
					   OWNER                        NVARCHAR(128)  '$.owner'
			          ,TABLE_NAME                   NVARCHAR(128)  '$.tableName'
			          ,COLUMN_LIST                  NVARCHAR(MAX)  '$.columns'
			          ,DATA_TYPE_LIST               NVARCHAR(MAX)  '$.dataTypes' as json
			          ,SIZE_CONSTRAINTS             NVARCHAR(MAX)  '$.sizeConstraints' as json
			          ,INSERT_SELECT_LIST           NVARCHAR(MAX)  '$.insertSelectList'
                      ,COLUMN_PATTERNS              NVARCHAR(MAX)  '$.columnPatterns'
                     ) v;
  
  SET QUOTED_IDENTIFIER ON; 
  BEGIN TRY
    OPEN FETCH_METADATA;
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

    WHILE @@FETCH_STATUS = 0 
    BEGIN 
      SET @ROW_COUNT = 0;
      SET @SQL_STATEMENT = JSON_VALUE(@STATEMENTS,'$.ddl')
      BEGIN TRY 
        EXEC(@SQL_STATEMENT)
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [ddl.tableName], @SQL_STATEMENT as [ddl.sqlStatement] 
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end TRY
      BEGIN CATCH  
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [error.tableName], @SQL_STATEMENT as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as 'msg'
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end CATCH
      
      BEGIN TRY 
        SET @START_TIME = SYSUTCDATETIME();
        SET @SQL_STATEMENT = JSON_VALUE(@STATEMENTS,'$.dml')
        EXEC(@SQL_STATEMENT)
        SET @ROW_COUNT = @@ROWCOUNT;
        SET @end_TIME = SYSUTCDATETIME();
        SET @ELAPSED_TIME = DATEDIFF(MILLISECOND,@START_TIME,@end_TIME);
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [dml.tableName], @ROW_COUNT as [dml.rowCount], @ELAPSED_TIME as [dml.elapsedTime], @SQL_STATEMENT  as [dml.sqlStatement]
             for JSON PATH, INCLUDE_NULL_VALUES
          )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end TRY  
      BEGIN CATCH  
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [error.tableName],@SQL_STATEMENT  as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as 'msg'
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end CATCH

      FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
    end;
   
    CLOSE FETCH_METADATA;
    DEALLOCATE FETCH_METADATA;
   
  end TRY 
  BEGIN CATCH
    SET @LOG_ENTRY = (
      select 'IMPORT_JSON' as [error.tableName], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as 'msg'
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  end CATCH
--
  SELECT @RESULTS;
end
--
GO
--
CREATE OR ALTER FUNCTION GENERATE_SQL(@TARGET_DATABASE VARCHAR(128),@METADATA NVARCHAR(MAX)) 
returns NVARCHAR(MAX)
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @STATEMENTS       NVARCHAR(MAX);
  
  DECLARE @RESULTS          NVARCHAR(MAX) = '{}'
                            
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select TABLE_NAME, 
         dbo.GENERATE_STATEMENTS(SOURCE_VENDOR, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_LIST, v.DATA_TYPE_LIST, v.SIZE_CONSTRAINTS) as STATEMENTS
  from  OPENJSON(@METADATA) 
         with (
           SOURCE_VENDOR nvarchar(128) '$.systemInformation.vendor'
          ,METADATA      nvarchar(max) '$.metadata' as json
         ) x
         cross apply OPENJSON(X.METADATA) y
		 cross apply OPENJSON(y.VALUE) 
		             with(
					   OWNER                        NVARCHAR(128)  '$.owner'
			          ,TABLE_NAME                   NVARCHAR(128)  '$.tableName'
			          ,COLUMN_LIST                  NVARCHAR(MAX)  '$.columns'
			          ,DATA_TYPE_LIST               NVARCHAR(MAX)  '$.dataTypes' as json
			          ,SIZE_CONSTRAINTS             NVARCHAR(MAX)  '$.sizeConstraints' as json
			          ,INSERT_SELECT_LIST           NVARCHAR(MAX)  '$.insertSelectList'
                      ,COLUMN_PATTERNS              NVARCHAR(MAX)  '$.columnPatterns'
                     ) v;
 
  SET QUOTED_IDENTIFIER ON; 
  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

  WHILE @@FETCH_STATUS = 0 
  BEGIN 
    SET @RESULTS = JSON_MODIFY(@RESULTS,concat('lax $."',@TABLE_NAME,'"'),@STATEMENTS)
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
  end;
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
    
  RETURN @RESULTS;
end
--
GO
--
CREATE OR ALTER PROCEDURE COMPARE_SCHEMA(@SOURCE_DATABASE NVARCHAR(128), @SOURCE_SCHEMA NVARCHAR(128), @TARGET_DATABASE NVARCHAR(128), @TARGET_SCHEMA NVARCHAR(128), @COMMENT NVARCHAR(2048)) 
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @COLUMN_LIST      NVARCHAR(MAX);
  DECLARE @SQL_STATEMENT    NVARCHAR(MAX);
  DECLARE @C_NEWLINE        CHAR(1) = CHAR(13);
  
  DECLARE @SOURCE_COUNT BIGINT;
  DECLARE @TARGET_COUNT BIGINT;
  
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select t.table_name
        ,string_agg(case 
                      when c.data_type in ('geography','geometry') then
                        concat('HASHBYTES(''SHA2_256'',"',c.column_name,'".ToString()) "',c.column_name,'"')
                      when c.data_type in ('xml','text','ntext') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.column_name,'" as NVARCHAR(MAX))) "',c.column_name,'"')
                      when c.data_type in ('image') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.column_name,'" as VARBINARY(MAX))) "',c.column_name,'"')
                      else  
                        concat('"',c.column_name,'"')
                      end
                   ,',') 
         within group (order by ordinal_position) "columns"
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = @SOURCE_SCHEMA
    -- and t.table_catalog = @SOURCE_DATABASE
  group by t.table_schema, t.table_name;
 
  SET QUOTED_IDENTIFIER ON; 
  
  select CONCAT( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'", "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'", ',@COMMENT) "Timestamp";
  
  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @COLUMN_LIST
  
  CREATE TABLE #SCHEMA_COMPARE_RESULTS (
    SOURCE_DATATBASE NVARCHAR(128)
   ,SOURCE_SCHEMA    NVARCHAR(128)
   ,TARGET_DATABASE  NVARCHAR(128)
   ,TARGET_SCHEMA    NVARCHAR(128)
   ,TABLE_NAME       NVARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSINGS_ROWS    INT
   ,EXTRA_ROWS       INT
  );

  WHILE @@FETCH_STATUS = 0 
  BEGIN 
    FETCH FETCH_METADATA INTO @TABLE_NAME, @COLUMN_LIST
    
    SET @SQL_STATEMENT = CONCAT('insert into #SCHEMA_COMPARE_RESULTS ',@C_NEWLINE,
                             ' select ''',@SOURCE_DATABASE,''' ',@C_NEWLINE,
                             '       ,''',@SOURCE_SCHEMA,''' ',@C_NEWLINE,
                             '       ,''',@TARGET_DATABASE,''' ',@C_NEWLINE,
                             '       ,''',@TARGET_SCHEMA,''' ',@C_NEWLINE,
                             '       ,''',@TABLE_NAME,''' ',@C_NEWLINE,
                             '       ,(select count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'")',@C_NEWLINE,
                             '       ,(select count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'")',@C_NEWLINE,
                             '       ,(select count(*) from (SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'") T1)',@C_NEWLINE,
                             '       ,(select count(*) from (SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'") T2)');
    
    EXEC(@SQL_STATEMENT);
   
  end;
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
    
  SELECT * 
    FROM #SCHEMA_COMPARE_RESULTS
end
--
GO
--
EXIT