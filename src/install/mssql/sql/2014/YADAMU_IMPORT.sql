use master
go
--
create type TYPE_MAPPING_TABLE
as table (
  VENDOR_TYPE NVARCHAR(256)
, MSSQL_TYPE NVARCHAR(256)
);
go
--							  
DECLARE @YADAMU_INSTANCE_ID VARCHAR(36);
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
IF OBJECT_ID('sp_YADAMU_INSTALLATION_TIMESTAMP') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_YADAMU_INSTALLATION_TIMESTAMP()
RETURNS VARCHAR(27)
as
begin
  return NULL
end
GO
--
set noexec off
go
--
DECLARE @YADAMU_INSTALLATION_TIMESTAMP VARCHAR(36);
BEGIN
  SET @YADAMU_INSTALLATION_TIMESTAMP = FORMAT(GetUtcDate(),'yyyy-MM-dd"T"HH:mm:sszzz')
  EXECUTE ('ALTER FUNCTION sp_YADAMU_INSTALLATION_TIMESTAMP() RETURNS VARCHAR(27) AS BEGIN RETURN ''' + @YADAMU_INSTALLATION_TIMESTAMP + ''' END');
END;
go
--
EXECUTE sp_ms_marksystemobject 'sp_YADAMU_INSTALLATION_TIMESTAMP'
go
--
IF OBJECT_ID('sp_JSON_ESCAPE') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_JSON_ESCAPE(@SQL_STRING NVARCHAR(MAX) )
returns NVARCHAR(MAX) 
as
begin
  return NULL
end
GO
--
set noexec off
go
--
ALTER FUNCTION sp_JSON_ESCAPE(@SQL_STRING NVARCHAR(MAX))
returns NVARCHAR(MAX) 
AS
begin
  declare @JSON_STRING NVARCHAR(MAX) = @SQL_STRING;
  with JSON_ESCAPE_CHARACTERS as (
    SELECT '\' AS UNESCAPED , '\\' AS ESCAPED
    UNION ALL SELECT '"', '\"' 
    UNION ALL SELECT '/', '\/'
    UNION ALL SELECT CHAR(08),'\b'
    UNION ALL SELECT CHAR(12),'\f'
    UNION ALL SELECT CHAR(10),'\n'
    UNION ALL SELECT CHAR(13),'\r'
    UNION ALL SELECT CHAR(09),'\t'
  )
  SELECT @JSON_STRING = REPLACE(@JSON_STRING, UNESCAPED, ESCAPED)
    FROM JSON_ESCAPE_CHARACTERS
  RETURN @JSON_STRING
end
go
--
-- " Fix Quote Mismatch in Notepad++ 
--
EXECUTE sp_ms_marksystemobject 'sp_JSON_ESCAPE'
go
--
IF OBJECT_ID('sp_MAP_MSSQL_DATATYPE') IS NOT NULL
   set noexec on
go
--
create function sp_MAP_MSSQL_DATATYPE(@VENDOR NVARCHAR(128), @MSSQL_DATA_TYPE NVARCHAR(256), @DATA_TYPE NVARCHAR(256), @DATA_TYPE_LENGTH BIGINT, @DATA_TYPE_SCALE INT, @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns VARCHAR(128) 
as
begin
  return null;
end
go
--
alter function sp_MAP_MSSQL_DATATYPE(@VENDOR NVARCHAR(128), @MSSQL_DATA_TYPE NVARCHAR(256), @DATA_TYPE NVARCHAR(256), @DATA_TYPE_LENGTH BIGINT, @DATA_TYPE_SCALE INT, @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
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
IF OBJECT_ID('sp_GENERATE_SQL') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_GENERATE_SQL(@VENDOR NVARCHAR(128), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_NAMES_ARRAY XML, @DATA_TYPES_ARARY XML, @SIZE_CONSTRAINTS_ARRAY XML, @SPATIAL_FORMAT NVARCHAR(128), @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns NVARCHAR(MAX)
as
begin
  return NULL
end
GO
--
set noexec off
go
--
ALTER FUNCTION sp_GENERATE_SQL(@VENDOR NVARCHAR(128), @TYPE_MAPPINGS NVARCHAR(MAX), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_NAMES_ARRAY XML, @DATA_TYPES_ARARY XML, @SIZE_CONSTRAINTS_ARRAY XML, @SPATIAL_FORMAT NVARCHAR(128), @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns NVARCHAR(MAX)
as
begin
  DECLARE @COLUMN_LIST        NVARCHAR(MAX);  
  DECLARE @COLUMNS_CLAUSE     NVARCHAR(MAX);
  DECLARE @TARGET_DATA_TYPES  NVARCHAR(MAX)
  
  DECLARE @DDL_STATEMENT      NVARCHAR(MAX);
  DECLARE @DML_STATEMENT      NVARCHAR(MAX);
  DECLARE @LEGACY_COLLATION   VARCHAR(256);
  declare @MAPPING_TABLE      TYPE_MAPPING_TABLE;
  DECLARE @XML_MAPPINGS       XML = CAST(@TYPE_MAPPINGS AS XML)
 
  SET @LEGACY_COLLATION = @DB_COLLATION;
  
  if (@LEGACY_COLLATION like '%_UTF8') begin
    SET @LEGACY_COLLATION = LEFT(@LEGACY_COLLATION, LEN(@LEGACY_COLLATION)-5);
  end;
 
  if (@LEGACY_COLLATION like '%_SC') begin
    SET @LEGACY_COLLATION = LEFT(@LEGACY_COLLATION, LEN(@LEGACY_COLLATION)-3);
  end;
  
  with TYPE_MAPPINGS as (
    select T."MAPPING_XML".query('vendorType').value('.','NVARCHAR(256)')   "VENDOR_TYPE"
          ,T."MAPPING_XML".query('mssqlrType').value('.','NVARCHAR(256)')   "MSSQL_TYPE"
	 from @XML_MAPPINGS.nodes('/typeMappings/typeMappongs') AS T("MAPPING_XML")
  )
  insert into @MAPPING_TABLE
  select * from TYPE_MAPPINGS;
  
  WITH "SOURCE_TABLE_DEFINITION" as (
    SELECT c."KEY" "INDEX"
          ,c."VALUE" "COLUMN_NAME"
		  ,d."VALUE" "DATA_TYPE"
		  ,case 
		     when (@VENDOR = 'MSSQLSERVER') then
			   d.VALUE
			 else
			   m."MSSQL_TYPE"
		   end "MSSQL_TYPE"	 
		  ,s."DATA_TYPE_LENGTH" 
		  ,S."DATA_TYPE_SCALE" 
      FROM ( 
	    select T."COLUMN_NAME".value('.', 'NVARCHAR(128)') as "VALUE", 
		      row_number() over(order by T.COLUMN_NAME) as "KEY" 
		 from @COLUMN_NAMES_ARRAY.nodes('/columnNames/columnName') as T("COLUMN_NAME")
	  ) c
	  JOIN ( 
		select T."DATA_TYPE".value('.', 'NVARCHAR(128)') as "VALUE", 
		       row_number() over(order by T.DATA_TYPE) as "KEY" 
		  from @DATA_TYPES_ARARY.nodes('/dataTypes/dataType') as T("DATA_TYPE")
	  ) d on c."KEY" = d."KEY"
	  JOIN (
		select T."SIZE_CONSTRAINT".value('./precision[1]/text()[1]','BIGINT') as "DATA_TYPE_LENGTH",
		       T."SIZE_CONSTRAINT".value('./scale[1]/text()[1]','BIGINT') as "DATA_TYPE_SCALE", 
		       row_number() over(order by T.SIZE_CONSTRAINT) as "KEY" 
		  from @SIZE_CONSTRAINTS_ARRAY.nodes('/sizeConstraints/sizeConstraint') as T("SIZE_CONSTRAINT")
	  ) s on c."KEY" = s."KEY"
      LEFT OUTER JOIN @MAPPING_TABLE m on lower(m.VENDOR_TYPE) = lower(d."VALUE")
  ),
  "TARGET_TABLE_DEFINITION" as (
    select st.*, master.dbo.sp_MAP_MSSQL_DATATYPE(@VENDOR, MSSQL_TYPE, "DATA_TYPE", "DATA_TYPE_LENGTH", "DATA_TYPE_SCALE", @CIRCLE_FORMAT, @DB_COLLATION) TARGET_DATA_TYPE
     from "SOURCE_TABLE_DEFINITION" st
  )
  SELECT @COLUMN_LIST = CONCAT(@COLUMN_LIST,CONCAT('"',"COLUMN_NAME",'"'),',')
        ,@COLUMNS_CLAUSE =
         CONCAT(@COLUMNS_CLAUSE,CONCAT('"',"COLUMN_NAME",'" ',
                           case
                             when "TARGET_DATA_TYPE" = 'boolean' then
                               'bit'
                             when "TARGET_DATA_TYPE" = 'json' then
                               -- CONCAT('nvarchar(max) CHECK(ISJSON("',"COLUMN_NAME",'") > 0)')
                               'nvarchar(max)'
                             when TARGET_DATA_TYPE LIKE '%(%)%' then
                               "TARGET_DATA_TYPE"
                             when "TARGET_DATA_TYPE" in('text','ntext')  then
                               CONCAT("TARGET_DATA_TYPE",' collate ',@LEGACY_COLLATION)
                             when "TARGET_DATA_TYPE" in('xml','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry','hierarchyid','uniqueidentifier')  then
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
                CONCAT(@TARGET_DATA_TYPES,CONCAT(
                                  '"',
                                  case
                                    when "TARGET_DATA_TYPE" LIKE '%(%)%' then
                                      "TARGET_DATA_TYPE"
                                    when "TARGET_DATA_TYPE" in ('json','xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry') then
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
      FROM "TARGET_TABLE_DEFINITION" tt;
   SET @COLUMNS_CLAUSE = LEFT(@COLUMNS_CLAUSE,LEN(@COLUMNS_CLAUSE)-1)
   SET @COLUMN_LIST = LEFT(@COLUMN_LIST,LEN(@COLUMN_LIST)-1)
   SET @TARGET_DATA_TYPES = LEFT(@TARGET_DATA_TYPES,LEN(@TARGET_DATA_TYPES)-1)
   
   SET @DDL_STATEMENT = CONCAT('if object_id(''"',@SCHEMA,'"."',@TABLE_NAME,'"'',''U'') is NULL create table "',@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMNS_CLAUSE,')');   
   -- Server Side processing of JSON not supported in SqlServer 2014
   SET @DML_STATEMENT = CONCAT('insert into "' ,@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMN_LIST,') select ');
   
   RETURN CONCAT('{"ddl": "',master.dbo.sp_JSON_ESCAPE(@DDL_STATEMENT),'", "dml": "',master.dbo.sp_JSON_ESCAPE(@DML_STATEMENT),'", "targetDataTypes": [',@TARGET_DATA_TYPES,']}')
end;
--
go
--
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_SQL'
go
--
IF OBJECT_ID('sp_GENERATE_STATEMENTS') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_GENERATE_STATEMENTS(@TARGET_DATABASE VARCHAR(128), @SPATIAL_FORMAT NVARCHAR(128), @METADATA NVARCHAR(MAX),@DB_COLLATION NVARCHAR(32)) 
returns NVARCHAR(MAX)
as
begin
  return NULL
end
GO
--
set noexec off
go

ALTER FUNCTION sp_GENERATE_STATEMENTS(@METADATA NVARCHAR(MAX), @TYPE_MAPPINGS NVARCHAR(MAX), @TARGET_DATABASE VARCHAR(128), @SPATIAL_FORMAT NVARCHAR(128), @DB_COLLATION NVARCHAR(32))
returns NVARCHAR(MAX)
as
begin
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @STATEMENTS       NVARCHAR(MAX);
  DECLARE @CIRCLE_FORMAT    NVARCHAR(7) = 'CIRCLE';
  DECLARE @RESULTS          NVARCHAR(MAX) = '{}';
 
  DECLARE @XMLDOC XML = CAST(@METADATA AS XML)
 
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  with TABLE_METADATA as (
    select T."TABLE_XML".query('vendor').value('.','NVARCHAR(128)')      "VENDOR"
          ,T."TABLE_XML".query('tableSchema').value('.','NVARCHAR(128)') "OWNER"
          ,T."TABLE_XML".query('tableName').value('.','NVARCHAR(128)')   "TABLE_NAME"
          ,T."TABLE_XML".query('columnNames')                            "COLUMN_NAMES_ARRAY"
          ,T."TABLE_XML".query('dataTypes')                              "DATA_TYPES_ARARY"
          ,T."TABLE_XML".query('sizeConstraints')                        "SIZE_CONSTRAINTS_ARRAY"
	 from @XMLDOC.nodes('/metadata/table') AS T("TABLE_XML")
  )
  select TABLE_NAME, 
         master.dbo.sp_GENERATE_SQL(VENDOR, @TYPE_MAPPINGS, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_NAMES_ARRAY, v.DATA_TYPES_ARARY, v.SIZE_CONSTRAINTS_ARRAY, @SPATIAL_FORMAT, @CIRCLE_FORMAT, @DB_COLLATION) as STATEMENTS
    from TABLE_METADATA v;
		
  SET QUOTED_IDENTIFIER ON; 
  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

  SET @RESULTS = '{';
  WHILE @@FETCH_STATUS = 0 
  begin 
    SET @RESULTS = CONCAT(@RESULTS,'"',@TABLE_NAME,'": ',@STATEMENTS,',')
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
  end;
  if (len(@RESULTS) > 1) begin
    set @RESULTS = LEFT(@RESULTS,LEN(@RESULTS)-1)
  end;
  SET @RESULTS = CONCAT(@RESULTS,'}');
   
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