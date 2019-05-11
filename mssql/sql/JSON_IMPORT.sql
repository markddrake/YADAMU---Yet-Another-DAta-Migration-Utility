use master
go
CREATE OR ALTER FUNCTION sp_MAP_FOREIGN_DATATYPE(@VENDOR NVARCHAR(128), @DATA_TYPE NVARCHAR(128), @DATA_TYPE_LENGTH BIGINT, @DATA_TYPE_SCALE INT) 
RETURNS VARCHAR(128) 
AS
BEGIN
  RETURN 
  case
    when @VENDOR = 'Oracle'
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
               then 'float(53)'
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
               then 'varchar(16)'                  
             when (CHARINDEX('TIMESTAMP',@DATA_TYPE) = 1) 
               then case
                      when (CHARINDEX('TIME ZONE',@DATA_TYPE) > 0) 
                        then 'datetimeoffset'
                      else 
                       'datetime2' 
               END
             when @DATA_TYPE = 'XMLTYPE'
               then 'xml'
             when @DATA_TYPE like '"%"."%"'
               then 'nvarchar(max)'
             when @DATA_TYPE = 'JSON' 
               then 'json'
             else
               lower(@DATA_TYPE)
           END
    when @VENDOR in ('MySQL','MariaDB')   
      then case 
             when @DATA_TYPE = 'mediumint' 
               then 'int'
             when @DATA_TYPE = 'datetime' 
               then 'datetime2'
             when @DATA_TYPE = 'timestamp' 
               then 'datetime2'
             when @DATA_TYPE = 'float' 
               then 'real'
             when @DATA_TYPE = 'double' 
               then 'float(53)'
             when @DATA_TYPE = 'enum'
               then 'varchar(255)'
             when @DATA_TYPE = 'set'
               then 'varchar(255)'
             when @DATA_TYPE = 'year'
               then 'smallint'
             when @DATA_TYPE = 'json' 
               then 'json'
             when @DATA_TYPE = 'blob' and @DATA_TYPE_LENGTH > 8000  
               then 'varbinary(max)'
             when @DATA_TYPE = 'blob' 
               then 'varbinary'
             when @DATA_TYPE = 'varchar' then
               -- For MySQL may need to add column character set to the table metadata object in order to accutately determine varchar Vs nvarchar ? 
               -- Alternatively the character set could be used when generating metadata from the MySQL dictionaly that distinguishes varchar from nvarchar even thought the dictionaly does not.
               'nvarchar'
             when @DATA_TYPE = 'text' then
               'nvarchar(max)'
             when @DATA_TYPE = 'mediumtext' then
               'nvarchar(max)'
             when @DATA_TYPE = 'longtext' then
               'nvarchar(max)'
             when @DATA_TYPE = 'longblob' then
               'varbinary(max)'
             when @DATA_TYPE = 'mediumblob' then
               'varbinary(max)'
             else
               lower(@DATA_TYPE)
           END
    when @VENDOR = 'Postgres'
      then case 
             when @DATA_TYPE = 'character varying' and @DATA_TYPE_LENGTH is null then
               'nvarchar(max)'
             when @DATA_TYPE = 'character varying' then
               'nvarchar'
             when @DATA_TYPE = 'character' then
               'nchar'
             when @DATA_TYPE = 'text' then
               'nvarchar(max)'
             when @DATA_TYPE = 'bytea' and @DATA_TYPE_LENGTH > 8000  then 
               'varbinary(max)'
             when @DATA_TYPE = 'bytea' then
               'varbinary'
             when @DATA_TYPE = 'boolean' then
               'bit'
             when @DATA_TYPE = 'timestamp' then
               'datetime'
             when @DATA_TYPE = 'timestamp with time zone' then
               'datetimeoffset'
             when @DATA_TYPE = 'timestamp without time zone' then
               'datetime2'
             when @DATA_TYPE = 'time without time zone' then
               'time'
             when (CHARINDEX('interval',@DATA_TYPE) = 1) then
               'varchar(64)'
             when @DATA_TYPE = 'double precision' then
               'float(53)'
             when @DATA_TYPE = 'real' then
               'real'
             when @DATA_TYPE = 'geometry' then
               'geometry'
             when @DATA_TYPE = 'geography'then
               'geometry'
             when @DATA_TYPE = 'integer' then
               'int'
             else
               lower(@DATA_TYPE)
           END          
    else 
      lower(@DATA_TYPE)
  END
END
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_MAP_FOREIGN_DATATYPE'
GO
--
CREATE OR ALTER FUNCTION sp_GENERATE_STATEMENTS(@VENDOR NVARCHAR(128), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_LIST NVARCHAR(MAX),@DATA_TYPE_LIST NVARCHAR(MAX),@DATA_SIZE_LIST NVARCHAR(MAX)) 
RETURNS NVARCHAR(MAX)
AS
BEGIN
  DECLARE @COLUMNS_CLAUSE     NVARCHAR(MAX);
  DECLARE @INSERT_SELECT_LIST NVARCHAR(MAX);
  DECLARE @WITH_CLAUSE        NVARCHAR(MAX);
  DECLARE @TARGET_DATA_TYPES  NVARCHAR(MAX)
  
  DECLARE @DDL_STATEMENT      NVARCHAR(MAX);
  DECLARE @DML_STATEMENT      NVARCHAR(MAX);
 
  WITH "SOURCE_TABLE_DEFINITION" as (
          SELECT c."KEY" "INDEX"
                ,c."VALUE" "COLUMN_NAME"
                ,t."VALUE" "DATA_TYPE"
                ,CAST(case
                        when s.VALUE = '' then
                          NULL
                        when CHARINDEX(',',s."VALUE") > 0 then
                          LEFT(s."VALUE",CHARINDEX(',',s."VALUE")-1)
                        else
                          s."VALUE"
                      END 
                      AS BIGINT) "DATA_TYPE_LENGTH"
                ,case
                   when CHARINDEX(',',s."VALUE") > 0  then 
                     RIGHT(s."VALUE", CHARINDEX(',',REVERSE(s."VALUE"))-1)
                   else 
                     NULL
                 END "DATA_TYPE_SCALE"
            FROM OPENJSON(CONCAT('[',@COLUMN_LIST,']')) c,
                 OPENJSON(@DATA_TYPE_LIST) t,
                 OPENJSON(@DATA_SIZE_LIST) s
           WHERE c."KEY" = t."KEY" and c."KEY" = s."KEY"
  ),
  "TARGET_TABLE_DEFINITION" as (
    select st.*, master.dbo.sp_MAP_FOREIGN_DATATYPE(@VENDOR, "DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
      from "SOURCE_TABLE_DEFINITION" st
  )
  SELECT @COLUMNS_CLAUSE =
         STRING_AGG(CONCAT('"',"COLUMN_NAME",'" ',
                           case
                             when "TARGET_DATA_TYPE" = 'json' then
                               'nvarchar(max)'
                             when TARGET_DATA_TYPE LIKE '%(%)%' then
                               "TARGET_DATA_TYPE"
                             when "TARGET_DATA_TYPE" in('xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry','hierarchyid','uniqueidentifier')  then
                               "TARGET_DATA_TYPE"                
                             when "DATA_TYPE_SCALE" IS NOT NULL then
                               CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                             when "DATA_TYPE_LENGTH"  IS NOT NULL  then
                               case 
                                 when "DATA_TYPE_LENGTH" = -1  then
                                   CONCAT("TARGET_DATA_TYPE",'(max)')
                                 else 
                                   CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                               END
                             else 
                               "TARGET_DATA_TYPE"
                           END
                         ) 
                   ,','                 
                   )
        ,@TARGET_DATA_TYPES =
         CONCAT('[',
                STRING_AGG(CONCAT(
                                  '"',
                                  case
                                    when "TARGET_DATA_TYPE" LIKE '%(%)%' then
                                      "TARGET_DATA_TYPE"
                                    when "TARGET_DATA_TYPE" in('json','xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney') then
                                      "TARGET_DATA_TYPE"                 
                                    when "DATA_TYPE_SCALE" IS NOT NULL then
                                      CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                                    when "DATA_TYPE_LENGTH"  IS NOT NULL  then
                                      case 
                                        when "DATA_TYPE_LENGTH" = -1  then
                                          CONCAT("TARGET_DATA_TYPE",'(max)')
                                        else 
                                          CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                                      END
                                    else 
                                      "TARGET_DATA_TYPE"
                                  END,
                                  '"'
                                 )
                           ,','                 
                          )
                ,']'
               )
        ,@INSERT_SELECT_LIST = 
         STRING_AGG(case
                      when "TARGET_DATA_TYPE" = 'image' then
                        CONCAT('CONVERT(image,CONVERT(varbinary(max),data."',"COLUMN_NAME",'",2))')
                      when "TARGET_DATA_TYPE" = 'varbinary(max)' then
                        CONCAT('CONVERT(varbinary(max),data."',"COLUMN_NAME",'",2)')
                      when "TARGET_DATA_TYPE" in ('binary','varbinary') then
                        case
                          when ((DATA_TYPE_LENGTH = -1) OR (DATA_TYPE = 'BLOB') or ((CHARINDEX('"."',DATA_TYPE) > 0))) then
                            CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(max),data."',"COLUMN_NAME",'",2) "',"COLUMN_NAME",'"')
                          else 
                            CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",'),data."',"COLUMN_NAME",'",2) "',"COLUMN_NAME",'"')
                        END
                      when "TARGET_DATA_TYPE" = 'geometry' then
                        CONCAT('GEOMETRY::STGeomFromText(data."',"COLUMN_NAME",'",0) "',"COLUMN_NAME",'"')
                      when "TARGET_DATA_TYPE" = 'geography' then
                        CONCAT('GEOGRAPHY::STGeomFromText(data."',"COLUMN_NAME",'",4326) "',"COLUMN_NAME",'"')
                      else
                        CONCAT('data."',"COLUMN_NAME",'"')
                    END 
                   ,','                 
                   )
       ,@WITH_CLAUSE =
        STRING_AGG(CONCAT('"',COLUMN_NAME,'" ',
                          case            
                            when "TARGET_DATA_TYPE" = 'json' then
                              'nvarchar(max)'                
                            when "TARGET_DATA_TYPE" = 'varbinary(max)' then
                              'varchar(max)'                
                            when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0) then
                              "TARGET_DATA_TYPE"
                            when "TARGET_DATA_TYPE" in('xml','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney') then
                              "TARGET_DATA_TYPE"         
                            when "TARGET_DATA_TYPE" = 'image' then
                              'varchar(max)'                
                            when "TARGET_DATA_TYPE" = 'text' then
                              'varchar(max)'                 
                            when "TARGET_DATA_TYPE" = 'ntext' then
                              'nvarchar(max)'                
                            when "TARGET_DATA_TYPE" = 'geography' then
                              'nvarchar(4000)'                
                            when "TARGET_DATA_TYPE" = 'geometry' then
                              'nvarchar(4000)'                
                            when "TARGET_DATA_TYPE" = 'hierarchyid' then
                              'nvarchar(4000)'                
                            when "TARGET_DATA_TYPE" in ('binary','varbinary') then
                              case
                                when "DATA_TYPE_LENGTH" < 0  then
                                  'varchar(max)'
                                else 
                                  CONCAT('varchar(',cast(("DATA_TYPE_LENGTH" * 2) as VARCHAR),')')
                              END
                            when "DATA_TYPE_SCALE" IS NOT NULL then
                              CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                            when "DATA_TYPE_LENGTH" IS NOT NULL  then
                              case 
                                when "DATA_TYPE_LENGTH" < 0  then
                                  CONCAT("TARGET_DATA_TYPE",'(max)')
                                else 
                                  CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                              END
                            else 
                              "TARGET_DATA_TYPE"
                          END,
                          ' ''$[',"INDEX",']'''
                        )
                        ,','                    
                   )
      FROM "TARGET_TABLE_DEFINITION" tt;
      
   SET @DDL_STATEMENT = CONCAT('if object_id(''"',@SCHEMA,'"."',@TABLE_NAME,'"'',''U'') is NULL create table "',@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMNS_CLAUSE,')');   
   SET @DML_STATEMENT = CONCAT('insert into "' ,@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMN_LIST,') select ',@INSERT_SELECT_LIST,'  from "#JSON_STAGING" s cross apply OPENJSON("DATA",''$.data."',@TABLE_NAME,'"'') with ( ',@WITH_CLAUSE,') data');
   RETURN JSON_MODIFY(JSON_MODIFY(JSON_MODIFY('{}','$.ddl',@DDL_STATEMENT),'$.dml',@DML_STATEMENT),'$.targetDataTypes',JSON_QUERY(@TARGET_DATA_TYPES))
END;
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_STATEMENTS'
GO
--
CREATE OR ALTER PROCEDURE sp_IMPORT_JSON(@TARGET_DATABASE VARCHAR(128)) 
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @STATEMENTS       NVARCHAR(MAX);
  DECLARE @SQL_STATEMENT    NVARCHAR(MAX);
  
  DECLARE @START_TIME       DATETIME2;
  DECLARE @END_TIME         DATETIME2;
  DECLARE @ELAPSED_TIME     BIGINT;  
  DECLARE @ROW_COUNT        BIGINT;
 
  DECLARE @LOG_ENTRY        NVARCHAR(MAX);
  DECLARE @RESULTS          NVARCHAR(MAX) = '[]';
                            
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select TABLE_NAME, 
         master.dbo.sp_GENERATE_STATEMENTS(VENDOR, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_LIST, v.DATA_TYPE_LIST, v.SIZE_CONSTRAINTS) as STATEMENTS
   from "#JSON_STAGING"
         cross apply OPENJSON("DATA") 
         with (
           VENDOR        nvarchar(128) '$.systemInformation.vendor'
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


  SET @SQL_STATEMENT = CONCAT('if not exists (select 1 from sys.schemas where name = N''',@TARGET_DATABASE,''') exec(''create schema "',@TARGET_DATABASE,'"'')')
  BEGIN TRY 
    EXEC(@SQL_STATEMENT)
    SET @LOG_ENTRY = (
      select @TARGET_DATABASE as [ddl.tableName], @SQL_STATEMENT as [ddl.sqlStatement] 
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  END TRY
  BEGIN CATCH  
    SET @LOG_ENTRY = (
      select 'FATAL' as 'error.severity', @TARGET_DATABASE as [error.tableName], @SQL_STATEMENT as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. State',ERROR_STATE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  END CATCH
      

  
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
      END TRY
      BEGIN CATCH  
        SET @LOG_ENTRY = (
          select 'FATAL' as 'error.severity', @TABLE_NAME as [error.tableName], @SQL_STATEMENT as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. State',ERROR_STATE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      END CATCH
      
      BEGIN TRY 
        SET @START_TIME = SYSUTCDATETIME();
        SET @SQL_STATEMENT = JSON_VALUE(@STATEMENTS,'$.dml')
        EXEC(@SQL_STATEMENT)
        SET @ROW_COUNT = @@ROWCOUNT;
        SET @END_TIME = SYSUTCDATETIME();
        SET @ELAPSED_TIME = DATEDIFF(MILLISECOND,@START_TIME,@END_TIME);
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [dml.tableName], @ROW_COUNT as [dml.rowCount], @ELAPSED_TIME as [dml.elapsedTime], @SQL_STATEMENT  as [dml.sqlStatement]
             for JSON PATH, INCLUDE_NULL_VALUES
          )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      END TRY  
      BEGIN CATCH  
        SET @LOG_ENTRY = (
          select 'FATAL' as 'error.severity', @TABLE_NAME as [error.tableName],@SQL_STATEMENT  as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      END CATCH

      FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
    END;
   
    CLOSE FETCH_METADATA;
    DEALLOCATE FETCH_METADATA;
   
  END TRY 
  BEGIN CATCH
    SET @LOG_ENTRY = (
      select 'FATAL' as 'error.severity', ERROR_PROCEDURE() as [error.tableName], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  END CATCH
--
  SELECT @RESULTS;
END
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_IMPORT_JSON'
GO
--
CREATE OR ALTER FUNCTION sp_GENERATE_SQL(@TARGET_DATABASE VARCHAR(128),@METADATA NVARCHAR(MAX)) 
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
         master.dbo.sp_GENERATE_STATEMENTS(VENDOR, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_LIST, v.DATA_TYPE_LIST, v.SIZE_CONSTRAINTS) as STATEMENTS
  from  OPENJSON(@METADATA) 
         with (
           METADATA      nvarchar(max) '$.metadata' as json
         ) x
         cross apply OPENJSON(X.METADATA) y
		 cross apply OPENJSON(y.VALUE) 
		             with(
					   VENDOR                       NVARCHAR(128)  '$.vendor'
			          ,OWNER                        NVARCHAR(128)  '$.owner'
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
  END;
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
    
  RETURN @RESULTS;
END
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_SQL'
GO
--
CREATE OR ALTER PROCEDURE sp_COMPARE_SCHEMA(@FORMAT_RESULTS BIT,@SOURCE_DATABASE NVARCHAR(128), @SOURCE_SCHEMA NVARCHAR(128), @TARGET_DATABASE NVARCHAR(128), @TARGET_SCHEMA NVARCHAR(128), @COMMENT NVARCHAR(2048), @EMPTY_STRING_IS_NULL BIT, @SPATIAL_PRECISION int) 
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @COLUMN_LIST      NVARCHAR(MAX);
  DECLARE @SQL_STATEMENT    NVARCHAR(MAX);
  DECLARE @C_NEWLINE        CHAR(1) = CHAR(10);
  
  DECLARE @SOURCE_COUNT BIGINT;
  DECLARE @TARGET_COUNT BIGINT;
  DECLARE @SQLERRM     NVARCHAR(2000);
  
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select t.table_name
        ,string_agg(case 
                      when c.data_type in ('varchar','nvarchar') then
                        case 
                          when @EMPTY_STRING_IS_NULL = 1 then
                            concat('case when "',c.column_name,'" = '''' then NULL else "',c.column_name,'" end "',c.column_name,'"')
                          else
                            concat('"',c.column_name,'"')
                        end
                      when c.data_type in ('geography','geometry') then
                        case 
                           when @SPATIAL_PRECISION is NULL then
                             concat('"',c.column_name,'".ToString()',c.column_name,'"')
                           else
                             case 
                               when c.data_type = 'geography' then
                                 concat('case when "',c.column_name,'".InstanceOf(''POINT'') = 1 then concat(str("',c.column_name,'".Long,18,',@SPATIAL_PRECISION,'),'','',str("',c.column_name,'".Lat,18,',@SPATIAL_PRECISION,')) else "',c.column_name,'".ToString() end "',c.column_name,'"')
                               else
                                 concat('case when "',c.column_name,'".InstanceOf(''POINT'') = 1 then concat(str("',c.column_name,'".STX,18,',@SPATIAL_PRECISION,'),'','',str("',c.column_name,'".STY,18,',@SPATIAL_PRECISION,')) else "',c.column_name,'".ToString() end "',c.column_name,'"')
                               end
                        end
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
  
  
  CREATE TABLE #SCHEMA_COMPARE_RESULTS (
    SOURCE_DATABASE  NVARCHAR(128)
   ,SOURCE_SCHEMA    NVARCHAR(128)
   ,TARGET_DATABASE  NVARCHAR(128)
   ,TARGET_SCHEMA    NVARCHAR(128)
   ,TABLE_NAME       NVARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSING_ROWS     INT
   ,EXTRA_ROWS       INT
   ,SQLERRM          NVARCHAR(2000)
  );

  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @COLUMN_LIST
  WHILE @@FETCH_STATUS = 0 
  BEGIN    
  
  
    SET @SQL_STATEMENT = CONCAT('insert into #SCHEMA_COMPARE_RESULTS ',@C_NEWLINE,
                             ' select ''',@SOURCE_DATABASE,''' ',@C_NEWLINE,
                             '       ,''',@SOURCE_SCHEMA,''' ',@C_NEWLINE,
                             '       ,''',@TARGET_DATABASE,''' ',@C_NEWLINE,
                             '       ,''',@TARGET_SCHEMA,''' ',@C_NEWLINE,
                             '       ,''',@TABLE_NAME,''' ',@C_NEWLINE,
                             '       ,(select count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'")',@C_NEWLINE,
                             '       ,(select count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'")',@C_NEWLINE,
                             '       ,(select count(*) from (SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'") T1)',@C_NEWLINE,
                             '       ,(select count(*) from (SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'") T2)',@C_NEWLINE,
                             '       ,NULL');
          

    BEGIN TRY 
      EXEC(@SQL_STATEMENT)
    END TRY
    BEGIN CATCH
      SET @SQLERRM = ERROR_MESSAGE()
      BEGIN TRY
        SET @SQL_STATEMENT = CONCAT('select @SOURCE_COUNT = count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"')
        EXEC sp_EXECUTESQL @SQL_STATEMENT ,'@COUNT INT OUTPUT', @SOURCE_COUNT OUT
      END TRY
      BEGIN CATCH
         SET @SQLERRM = ERROR_MESSAGE()
         SET @SOURCE_COUNT = -1
      END CATCH
      BEGIN TRY
        SET @SQL_STATEMENT = CONCAT('select @TARGET_COUNT = count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"')
        EXEC sp_EXECUTESQL @SQL_STATEMENT,'@COUNT INT OUTPUT', @TARGET_COUNT OUT
      END TRY
      BEGIN CATCH
         SET @SQLERRM = ERROR_MESSAGE()
         SET @TARGET_COUNT = -1
      END CATCH
      INSERT INTO #SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, -1, -1, @SQLERRM)
    END CATCH;
    FETCH FETCH_METADATA INTO @TABLE_NAME, @COLUMN_LIST
  END
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
  
  IF (@FORMAT_RESULTS = 1) 
  BEGIN

    SELECT CAST(CONCAT( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "',@SOURCE_DATABASE,'","',@TARGET_DATABASE,'", ',@COMMENT) as NVARCHAR(256)) "Timestamp"
  
    SELECT CAST(FORMATMESSAGE('%32s %32s %48s SUCCESSFUL %12i', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT) as NVARCHAR(256)) "Successs"
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
--
    SELECT CAST(FORMATMESSAGE('%32s %32s %48s FAILED     %12i %12i %12i %12i %64s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM) as NVARCHAR(256)) "Failed"
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
        or MISSING_ROWS <> 0
       or EXTRA_ROWS <> 0
        or SQLERRM is not NULL
     order by TABLE_NAME;
  END
  ELSE 
  BEGIN
    SELECT SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
  
    SELECT SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
        or MISSING_ROWS <> 0
       or EXTRA_ROWS <> 0
        or SQLERRM is not NULL
     order by TABLE_NAME;
  END
--
END
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_COMPARE_SCHEMA'
GO
--
EXIT