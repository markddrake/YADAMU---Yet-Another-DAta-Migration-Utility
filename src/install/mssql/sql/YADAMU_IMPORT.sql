use master
use master
go
drop function if exists sp_MAP_FOREIGN_DATATYPE
go
drop function if exists sp_GENERATE_STATEMENTS
go
drop procedure if exists sp_YADAMU_IMPORT
go
drop procedure if exists sp_IMPORT_JSON
go
drop function if exists sp_SET_TYPE_MAPPINGS
go
drop function if exists sp_MAP_MSSQL_DATATYPE
go
drop type if exists TYPE_MAPPING_TABLE
--
create type TYPE_MAPPING_TABLE
as table (
  VENDOR_TYPE NVARCHAR(256)
, MSSQL_TYPE NVARCHAR(256)
);
go
--
declare @YADAMU_INSTANCE_ID VARCHAR(36);
BEGIN
  BEGIN TRY
    EXECUTE @YADAMU_INSTANCE_ID = sp_YADAMU_INSTANCE_ID;
  END TRY
  BEGIN CATCH
    SET @YADAMU_INSTANCE_ID = NEWID();
    EXECUTE ('CREATE FUNCTION sp_YADAMU_INSTANCE_ID() RETURNS VARCHAR(36) AS BEGIN RETURN ''' + @YADAMU_INSTANCE_ID + ''' END');
  END CATCH
END;
go
--
EXECUTE sp_ms_marksystemobject 'sp_YADAMU_INSTANCE_ID'
go
declare @YADAMU_INSTALLATION_TIMESTAMP VARCHAR(36);
BEGIN
  SET @YADAMU_INSTALLATION_TIMESTAMP = FORMAT(GetUtcDate(),'yyyy-MM-dd"T"HH:mm:sszzz')
  EXECUTE ('CREATE or alter function sp_YADAMU_INSTALLATION_TIMESTAMP() RETURNS VARCHAR(27) AS BEGIN RETURN ''' + @YADAMU_INSTALLATION_TIMESTAMP + ''' END');
END;
go
--
EXECUTE sp_ms_marksystemobject 'sp_YADAMU_INSTALLATION_TIMESTAMP'
go
create or alter function sp_MAP_MSSQL_DATATYPE(@VENDOR NVARCHAR(128), @MSSQL_DATA_TYPE NVARCHAR(256), @DATA_TYPE NVARCHAR(256), @DATA_TYPE_LENGTH BIGINT, @DATA_TYPE_SCALE INT, @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns VARCHAR(128) 
as
begin
  -- Force Larger CHAR and NCHAR to NVARCHAR to reduce likely-hood of hitting row size limit....
  declare @CHAR_LENGTH             NUMERIC(6) = 2000;
  declare @NCHAR_LENGTH            NUMERIC(6) = 2000;
  declare @VARCHAR_LENGTH          NUMERIC(6) = 8000;
  declare @NVARCHAR_LENGTH         NUMERIC(6) = 4000;
  declare @BINARY_LENGTH           NUMERIC(6) = 8000;
  declare @VARBINARY_LENGTH        NUMERIC(6) = 8000;
							       
  declare @CLOB_TYPE               VARCHAR(32) = 'varchar(max)';
  declare @ORACLE_OBJECT_TYPE      VARCHAR(32) = @CLOB_TYPE;
  declare @BLOB_TYPE               VARCHAR(32) = 'varbinary(max)';
  declare @NCLOB_TYPE              VARCHAR(32) = 'nvarchar(max)';
  declare @UNBOUNDED_NUMERIC_TYPE  VARCHAR(32) = 'numeric(38,19)';
  declare @MAX_NUMERIC_TYPE        VARCHAR(32) = 'numeric(38,0)';
  
  declare @TYPE_NOT_FOUND         NVARCHAR(256) = concat('MSSQLSERVER: ','Missing mapping for "',@VENDOR,'" datatype "',@DATA_TYPE,'"');
 
  if (CHARINDEX('(max)',@MSSQL_DATA_TYPE) > 0) begin
    set @MSSQL_DATA_TYPE = LEFT(@MSSQL_DATA_TYPE, LEN(@MSSQL_DATA_TYPE)-5)
  end;
  
  return case                                                       
    when ((@MSSQL_DATA_TYPE is NULL) and (@VENDOR = 'Oracle') and (@DATA_TYPE like '"%"."%"')) then @ORACLE_OBJECT_TYPE
	when (@MSSQL_DATA_TYPE is NULL)                                                            then @TYPE_NOT_FOUND
    when (@MSSQL_DATA_TYPE = 'nchar')                                                          then
	  case                                                                                     
	    when @DATA_TYPE_LENGTH = -1                                                            then @NCLOB_TYPE
	    when @DATA_TYPE_LENGTH is NULL                                                         then @NCLOB_TYPE
		when (@DATA_TYPE_LENGTH > @NVARCHAR_LENGTH)                                            then @NCLOB_TYPE
		when (@DATA_TYPE_LENGTH > @NCHAR_LENGTH)                                               then 'nvarchar'
                                                                                               else @MSSQL_DATA_TYPE
	  end                                                                                      
    when (@MSSQL_DATA_TYPE = 'nvarchar')                                                       then
	  case                                                                                     
	    when @DATA_TYPE_LENGTH = -1                                                            then @NCLOB_TYPE
	    when @DATA_TYPE_LENGTH is NULL                                                         then @NCLOB_TYPE
		when (@DATA_TYPE_LENGTH > @NVARCHAR_LENGTH)                                            then @NCLOB_TYPE
                                                                                               else @MSSQL_DATA_TYPE
	  end                                                                                      
    when (@MSSQL_DATA_TYPE = 'char')                                                           then
	  case                                                                                     
	    when @DATA_TYPE_LENGTH = -1                                                            then @CLOB_TYPE
	    when @DATA_TYPE_LENGTH is NULL                                                         then @CLOB_TYPE
		when (@DATA_TYPE_LENGTH > @VARCHAR_LENGTH)                                             then @CLOB_TYPE
		when (@DATA_TYPE_LENGTH > @CHAR_LENGTH)                                                then 'varchar'
                                                                                               else @MSSQL_DATA_TYPE
	  end                                                                                      
    when (@MSSQL_DATA_TYPE = 'varchar')                                                        then
	  case                                                                                     
	    when @DATA_TYPE_LENGTH = -1                                                            then @CLOB_TYPE
	    when @DATA_TYPE_LENGTH is NULL                                                         then @CLOB_TYPE
		when (@DATA_TYPE_LENGTH > @VARCHAR_LENGTH)                                             then @CLOB_TYPE
                                                                                               else @MSSQL_DATA_TYPE
	  end                                                                                      
    when (@MSSQL_DATA_TYPE = 'binary')                                                         then
	  case                                                                                     
	    when @DATA_TYPE_LENGTH = -1                                                            then @BLOB_TYPE
	    when @DATA_TYPE_LENGTH is NULL                                                         then @BLOB_TYPE
		when (@DATA_TYPE_LENGTH > @BINARY_LENGTH)                                              then @BLOB_TYPE
		when (@DATA_TYPE_LENGTH > @CHAR_LENGTH)                                                then 'varbinary'
                                                                                               else @MSSQL_DATA_TYPE
	  end                                                                                      
    when (@MSSQL_DATA_TYPE = 'varbinary')                                                      then
	  case                                                                                     
	    when @DATA_TYPE_LENGTH = -1                                                            then @BLOB_TYPE
	    when @DATA_TYPE_LENGTH is NULL                                                         then @BLOB_TYPE
		when (@DATA_TYPE_LENGTH > @VARBINARY_LENGTH)                                           then @BLOB_TYPE
                                                                                               else @MSSQL_DATA_TYPE
	  end                                                                                      
    when (@MSSQL_DATA_TYPE = 'text')                                                           then @CLOB_TYPE
	when (@MSSQL_DATA_TYPE = 'ntext')                                                          then @NCLOB_TYPE
    when (@MSSQL_DATA_TYPE = 'image')                                                          then @BLOB_TYPE
    when @MSSQL_DATA_TYPE in (                                                                       
          'numeric',                                                                           
          'decimal'                                                                            
      )                                                                                        then
      case                                                                                    
	    when (@DATA_TYPE_LENGTH is NULL)                                                       then @UNBOUNDED_NUMERIC_TYPE
        when (@DATA_TYPE_LENGTH > 38) and (@DATA_TYPE_SCALE = 0)                               then @MAX_NUMERIC_TYPE
        when (@DATA_TYPE_LENGTH > 38) and (@DATA_TYPE_SCALE > 0)                               then concat('numeric(38,',cast(round(@DATA_TYPE_SCALE*(38.0/@DATA_TYPE_LENGTH),0) as NUMERIC(2)),')')
                                                                                               else @MSSQL_DATA_TYPE
      end                                                                                     
                                                                                               else @MSSQL_DATA_TYPE
  end
end;
--
go
--
EXECUTE sp_ms_marksystemobject 'sp_MAP_MSSQL_DATATYPE'
go
--
create or alter function sp_GENERATE_SQL(@VENDOR NVARCHAR(128), @TYPE_MAPPINGS NVARCHAR(MAX), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_NAMES_ARRAY NVARCHAR(MAX),@DATA_TYPES_ARRAY NVARCHAR(MAX),@SIZE_CONSTRAINTS_ARRAY NVARCHAR(MAX), @SPATIAL_FORMAT NVARCHAR(128), @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns NVARCHAR(MAX)
as
begin
  declare @COLUMN_LIST        NVARCHAR(MAX);  
  declare @COLUMNS_CLAUSE     NVARCHAR(MAX);
  declare @INSERT_SELECT_LIST NVARCHAR(MAX);
  declare @WITH_CLAUSE        NVARCHAR(MAX);
  declare @TARGET_DATA_TYPES  NVARCHAR(MAX)
  
  declare @DDL_STATEMENT      NVARCHAR(MAX);
  declare @DML_STATEMENT      NVARCHAR(MAX);
  declare @LEGACY_COLLATION   VARCHAR(256);

  declare @VARBINARY_LENGTH   NUMERIC(6) = 8000;
  
  declare @CLOB_TYPE          VARCHAR(32) = 'varchar(max)';
  declare @BLOB_TYPE          VARCHAR(32) = 'varbinary(max)';
  declare @NCLOB_TYPE         VARCHAR(32) = 'nvarchar(max)';
 
  declare @MAPPING_TABLE      TYPE_MAPPING_TABLE;
  
  SET @LEGACY_COLLATION = @DB_COLLATION;
  
  if (@LEGACY_COLLATION like '%_UTF8') begin
    SET @LEGACY_COLLATION = LEFT(@LEGACY_COLLATION, LEN(@LEGACY_COLLATION)-5);
  end;
 
  if (@LEGACY_COLLATION like '%_SC') begin
    SET @LEGACY_COLLATION = LEFT(@LEGACY_COLLATION, LEN(@LEGACY_COLLATION)-3);
  end;
  
  insert into @MAPPING_TABLE
  select m.VENDOR_TYPE, m.MSSQL_TYPE 
    from OPENJSON(@TYPE_MAPPINGS)
         with (
           VENDOR_TYPE     NVARCHAR(256) '$[0]'
         , MSSQL_TYPE      NVARCHAR(256) '$[1]'
         ) m; 
  
  WITH "SOURCE_TABLE_DEFINITION" as (
    SELECT c."KEY" "INDEX"
          ,c."VALUE" "COLUMN_NAME"
          ,t."VALUE" "DATA_TYPE"
		, case 
		    when (@VENDOR = 'MSSQLSERVER') then
			   t.VALUE
			 else
			   m."MSSQL_TYPE"
		   end "MSSQL_TYPE"	 
          ,CAST(case
                  when s.VALUE = '' then
                    NULL
                  when CHARINDEX(',',s."VALUE") > 0 then
                    LEFT(s."VALUE",CHARINDEX(',',s."VALUE")-1)
                  else
                    s."VALUE"
                end 
                as BIGINT) "DATA_TYPE_LENGTH"
          ,case
             when CHARINDEX(',',s."VALUE") > 0  then 
               RIGHT(s."VALUE", CHARINDEX(',',REVERSE(s."VALUE"))-1)
             else 
               NULL
           end "DATA_TYPE_SCALE"
      FROM OPENJSON(@COLUMN_NAMES_ARRAY) c
      JOIN OPENJSON(@DATA_TYPES_ARRAY) t on c."KEY" = t."KEY"
      JOIN OPENJSON(@SIZE_CONSTRAINTS_ARRAY) s on c."KEY" = s."KEY"    
      LEFT OUTER JOIN @MAPPING_TABLE m on lower(m.VENDOR_TYPE) = lower(t."VALUE")
  ),
  "TARGET_TABLE_DEFINITION" as (
    select st.*, master.dbo.sp_MAP_MSSQL_DATATYPE(@VENDOR, "MSSQL_TYPE", "DATA_TYPE", "DATA_TYPE_LENGTH", "DATA_TYPE_SCALE", @CIRCLE_FORMAT, @DB_COLLATION) TARGET_DATA_TYPE
      from "SOURCE_TABLE_DEFINITION" st
  )
  SELECT @COLUMN_LIST = STRING_AGG(CONCAT('"',"COLUMN_NAME",'"'),',')
        ,@COLUMNS_CLAUSE =
         STRING_AGG(CONCAT('"',"COLUMN_NAME",'" ',
                           case
                             when "TARGET_DATA_TYPE" = 'boolean' then
                               'bit'
                             when "TARGET_DATA_TYPE" = 'json' then
                               CONCAT('nvarchar(max) CHECK(ISJSON("',"COLUMN_NAME",'") > 0)')
                             when TARGET_DATA_TYPE LIKE '%(%)%' then
                               "TARGET_DATA_TYPE"
                             when "TARGET_DATA_TYPE" in('text','ntext')  then
                               CONCAT("TARGET_DATA_TYPE",' collate ',@LEGACY_COLLATION)
                             when "TARGET_DATA_TYPE" in('xml','image','real','float','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry','hierarchyid','uniqueidentifier')  then
                               "TARGET_DATA_TYPE"                
                             when "DATA_TYPE_SCALE" IS NOT NULL then
                               CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                             when "DATA_TYPE_LENGTH"  IS NOT NULL  then
                               case 
                                 when "DATA_TYPE_LENGTH" = -1  then
                                   CONCAT("TARGET_DATA_TYPE",'(max)')
                                 else 
                                   CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
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
                                    when "TARGET_DATA_TYPE" LIKE '%(%)%' then
                                      "TARGET_DATA_TYPE"
                                    when "TARGET_DATA_TYPE" in ('json','xml','text','ntext','image','real','float','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry') then
                                      "TARGET_DATA_TYPE"                 
                                    when "DATA_TYPE_SCALE" IS NOT NULL then
                                      CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                                    when "DATA_TYPE_LENGTH"  IS NOT NULL  then
                                      case 
                                        when "DATA_TYPE_LENGTH" = -1  then
                                          CONCAT("TARGET_DATA_TYPE",'(max)')
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
                        end
                      when "TARGET_DATA_TYPE" = 'geometry'  then
                        case
                          when @SPATIAL_FORMAT in ('WKT','EWKT') then
                            CONCAT('geometry::STGeomFromText(data."',"COLUMN_NAME",'",4326) "',"COLUMN_NAME",'"')
                          when @SPATIAL_FORMAT in ('WKB','EWKB') then
                            CONCAT('geometry::STGeomFromWKB(CONVERT(varbinary(max),data."',"COLUMN_NAME",'",2),4326) "',"COLUMN_NAME",'"')
                          else                            
                            CONCAT('geometry::STGeomFromWKB(CONVERT(varbinary(max),data."',"COLUMN_NAME",'",2),4326) "',"COLUMN_NAME",'"')
                        end
                      when "TARGET_DATA_TYPE" = 'geography' then
                        case
                          when @SPATIAL_FORMAT in ('WKT','EWKT') then
                            CONCAT('geography::STGeomFromText(data."',"COLUMN_NAME",'",4326) "',"COLUMN_NAME",'"')
                          when @SPATIAL_FORMAT in ('WKB','EWKB') then
                            CONCAT('geography::STGeomFromWKB(CONVERT(varbinary(max),data."',"COLUMN_NAME",'",2),4326) "',"COLUMN_NAME",'"')
                          else                            
                            CONCAT('geography::STGeomFromWKB(CONVERT(varbinary(max),data."',"COLUMN_NAME",'",2),4326) "',"COLUMN_NAME",'"')
                        end
                      else
                        CONCAT('data."',"COLUMN_NAME",'"')
                    end 
                   ,','                 
                   )
       ,@WITH_CLAUSE =
        STRING_AGG(CONCAT('"',COLUMN_NAME,'" ',
                          case            
                            when "TARGET_DATA_TYPE" = 'json' then
                              'nvarchar(max)'                
                            when "TARGET_DATA_TYPE" = @BLOB_TYPE then
							  @CLOB_TYPE
                            when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0) then
                              "TARGET_DATA_TYPE"
                            when "TARGET_DATA_TYPE" in('xml','real','float','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney') then
                              "TARGET_DATA_TYPE"         
                            when "TARGET_DATA_TYPE" = 'image' then
                              'varchar(max)'                
                            when "TARGET_DATA_TYPE" = 'text' then
                              'varchar(max)'                 
                            when "TARGET_DATA_TYPE" = 'ntext' then
                              'nvarchar(max)'                
                            when "TARGET_DATA_TYPE" = 'geography' then
                              @NCLOB_TYPE                 
                            when "TARGET_DATA_TYPE" = 'geometry' then
                              @NCLOB_TYPE
                            when "TARGET_DATA_TYPE" = 'hierarchyid' then
                              @NCLOB_TYPE
                            when "TARGET_DATA_TYPE" in ('binary','varbinary') then
                              case
                                when "DATA_TYPE_LENGTH" < 0  then
								  @CLOB_TYPE
                                else 
                                  CONCAT('varchar(',@VARBINARY_LENGTH,')')
                              end
                            when "DATA_TYPE_SCALE" IS NOT NULL then
                              CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
                            when "DATA_TYPE_LENGTH" IS NOT NULL  then
                              case 
                                when "DATA_TYPE_LENGTH" < 0  then
                                  CONCAT("TARGET_DATA_TYPE",'(max)')
                                else 
                                  CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                              end
                            else 
                              "TARGET_DATA_TYPE"
                          end,
                          ' ''$[',"INDEX",']'''
                        )
                        ,','                    
                   )
      FROM "TARGET_TABLE_DEFINITION" tt;
      
   SET @DDL_STATEMENT = CONCAT('if object_id(''"',@SCHEMA,'"."',@TABLE_NAME,'"'',''U'') is NULL create table "',@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMNS_CLAUSE,')');   
   SET @DML_STATEMENT = CONCAT('insert into "' ,@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMN_LIST,') select ',@INSERT_SELECT_LIST,'  from "#YADAMU_STAGING" s cross apply OPENJSON("DATA",''$.data."',@TABLE_NAME,'"'') with ( ',@WITH_CLAUSE,') data');
   RETURN JSON_MODIFY(JSON_MODIFY(JSON_MODIFY('{}','$.ddl',@DDL_STATEMENT),'$.dml',@DML_STATEMENT),'$.targetDataTypes',JSON_QUERY(@TARGET_DATA_TYPES))
end;
--
go
--
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_SQL'
go
--
create or alter procedure sp_YADAMU_IMPORT(@TARGET_DATABASE VARCHAR(128),@DB_COLLATION VARCHAR(32)) 
as
begin
  declare @OWNER            VARCHAR(128);
  declare @TABLE_NAME       VARCHAR(128);
  declare @STATEMENTS       NVARCHAR(MAX);
  declare @SQL_STATEMENT    NVARCHAR(MAX);
  
  declare @START_TIME       DATETIME2;
  declare @END_TIME         DATETIME2;
  declare @ELAPSED_TIME     BIGINT;  
  declare @ROW_COUNT        BIGINT;
 
  declare @LOG_ENTRY        NVARCHAR(MAX);
  declare @RESULTS          NVARCHAR(MAX) = '[]';
  
  declare FETCH_METADATA 
  CURSOR FOR 
  select TABLE_NAME, 
         master.dbo.sp_GENERATE_STATEMENTS(VENDOR, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_NAMES_ARRAY, v.DATA_TYPES_ARRAY, v.SIZE_CONSTRAINTS_ARRAY, SPATIAL_FORMAT, CIRCLE_FORMAT, @DB_COLLATION) as STATEMENTS
   from "#YADAMU_STAGING"
         cross apply OPENJSON("DATA") 
         with (
           VENDOR         nvarchar(128) '$.systemInformation.vendor'
          ,SPATIAL_FORMAT nvarchar(128) '$.systemInformation.typeMappings.spatialFormat'
          ,CIRCLE_FORMAT  nvarchar(7)   '$.systemInformation.typeMappings.circleFormat'
          ,METADATA       nvarchar(max) '$.metadata' as json
         ) x
         cross apply OPENJSON(x.METADATA) y
         cross apply OPENJSON(y.value) 
                     with (
                       OWNER                        NVARCHAR(128)  '$.owner'
                      ,TABLE_NAME                   NVARCHAR(128)  '$.tableName'
                      ,COLUMN_NAMES_ARRAY           NVARCHAR(MAX)  '$.columnNames' as json
                      ,DATA_TYPES_ARRAY             NVARCHAR(MAX)  '$.dataTypes' as json
                      ,SIZE_CONSTRAINTS_ARRAY       NVARCHAR(MAX)  '$.sizeConstraints' as json
                     ) v;

  SET @SQL_STATEMENT = CONCAT('if not exists (select 1 from sys.schemas where name = N''',@TARGET_DATABASE,''') exec(''create schema "',@TARGET_DATABASE,'"'')')
  begin TRY 
    EXEC(@SQL_STATEMENT)
    SET @LOG_ENTRY = (
      select @TARGET_DATABASE as [ddl.tableName], @SQL_STATEMENT as [ddl.sqlStatement] 
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  end TRY
  begin CATCH  
    SET @LOG_ENTRY = (
      select 'FATAL' as 'error.severity', @TARGET_DATABASE as [error.tableName], @SQL_STATEMENT as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. State',ERROR_STATE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  end CATCH
        
  SET QUOTED_IDENTIFIER ON; 
  begin TRY
    OPEN FETCH_METADATA;
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

    WHILE @@FETCH_STATUS = 0 
    begin 
      SET @ROW_COUNT = 0;
      SET @SQL_STATEMENT = JSON_VALUE(@STATEMENTS,'$.ddl')
      begin TRY 
        EXEC(@SQL_STATEMENT)
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [ddl.tableName], @SQL_STATEMENT as [ddl.sqlStatement] 
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end TRY
      begin CATCH  
        SET @LOG_ENTRY = (
          select 'FATAL' as 'error.severity', @TABLE_NAME as [error.tableName], @SQL_STATEMENT as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. State',ERROR_STATE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end CATCH
      
      begin TRY 
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
      end TRY  
      begin CATCH  
        SET @LOG_ENTRY = (
          select 'FATAL' as 'error.severity', @TABLE_NAME as [error.tableName],@SQL_STATEMENT  as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
      end CATCH

      FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
    end;
   
    CLOSE FETCH_METADATA;
    DEALLOCATE FETCH_METADATA;
   
  end TRY 
  begin CATCH
    SET @LOG_ENTRY = (
      select 'FATAL' as 'error.severity', ERROR_PROCEDURE() as [error.tableName], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as [error.msg], CONCAT(ERROR_PROCEDURE(),'. Line: ', ERROR_LINE(),'. Severity:',ERROR_SEVERITY(),'.') as [error.details]
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    SET @RESULTS = JSON_MODIFY(@RESULTS,'append $',JSON_QUERY(@LOG_ENTRY,'$[0]'))
  end CATCH
--
  SELECT @RESULTS;
end
--
go
--
EXECUTE sp_ms_marksystemobject 'sp_YADAMU_IMPORT'
go
--
create or alter function sp_GENERATE_STATEMENTS(@METADATA NVARCHAR(MAX), @TYPE_MAPPINGS NVARCHAR(MAX), @TARGET_DATABASE VARCHAR(128), @SPATIAL_FORMAT NVARCHAR(128), @DB_COLLATION NVARCHAR(32))
returns NVARCHAR(MAX)
as
begin
  declare @OWNER               VARCHAR(128);
  declare @TABLE_NAME          VARCHAR(128);
  declare @STATEMENTS          NVARCHAR(MAX);
  declare @CIRCLE_FORMAT       NVARCHAR(7) = 'CIRCLE';
  declare @RESULTS             NVARCHAR(MAX) = '{}';
             			 
  declare FETCH_METADATA 
  cursor for
  select TABLE_NAME, 
         master.dbo.sp_GENERATE_SQL(VENDOR, @TYPE_MAPPINGS, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_NAMES_ARRAY, v.DATA_TYPES_ARRAY, v.SIZE_CONSTRAINTS_ARRAY, @SPATIAL_FORMAT, @CIRCLE_FORMAT, @DB_COLLATION) as STATEMENTS
  from  OPENJSON(@METADATA) 
         with (
           METADATA      nvarchar(max) '$.metadata' as json
         ) x
         cross apply OPENJSON(x.METADATA) y
         cross apply OPENJSON(y.value) 
                     with(
                       VENDOR                       NVARCHAR(128)  '$.vendor'
                      ,OWNER                        NVARCHAR(128)  '$.owner'
                      ,TABLE_NAME                   NVARCHAR(128)  '$.tableName'
                      ,COLUMN_NAMES_ARRAY           NVARCHAR(MAX)  '$.columnNames' as json
                      ,DATA_TYPES_ARRAY             NVARCHAR(MAX)  '$.dataTypes' as json
                      ,SIZE_CONSTRAINTS_ARRAY       NVARCHAR(MAX)  '$.sizeConstraints' as json
                     ) v;
 
  SET QUOTED_IDENTIFIER ON; 
  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

  WHILE @@FETCH_STATUS = 0 
  begin 
    SET @RESULTS = JSON_MODIFY(@RESULTS,concat('lax $."',@TABLE_NAME,'"'),JSON_Query(@STATEMENTS))
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
  end;
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
    
  RETURN @RESULTS;
end
--
go
--
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_STATEMENTS'
go
--
select master.dbo.sp_YADAMU_INSTANCE_ID() "YADAMU_INSTANCE_ID", master.dbo.sp_YADAMU_INSTALLATION_TIMESTAMP() "YADAMU_INSTALLATION_TIMESTAMP";
go
--
EXIT