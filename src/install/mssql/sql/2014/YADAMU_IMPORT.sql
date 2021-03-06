use master
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
IF OBJECT_ID('sp_MAP_FOREIGN_DATATYPE') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_MAP_FOREIGN_DATATYPE(@VENDOR NVARCHAR(128), @DATA_TYPE NVARCHAR(128), @DATA_TYPE_LENGTH BIGINT, @DATA_TYPE_SCALE INT, @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns VARCHAR(128) 
as
begin
  return NULL
end
GO
--
set noexec off
go
--
ALTER FUNCTION sp_MAP_FOREIGN_DATATYPE(@VENDOR NVARCHAR(128), @DATA_TYPE NVARCHAR(128), @DATA_TYPE_LENGTH BIGINT, @DATA_TYPE_SCALE INT, @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns VARCHAR(128) 
as
begin
  declare @LARGEST_CHAR_SIZE      NUMERIC(6) = 2000;
  declare @LARGEST_NCHAR_SIZE     NUMERIC(6) = 2000;
  declare @LARGEST_VARCHAR_SIZE   NUMERIC(6) = 8000;
  declare @LARGEST_NVARCHAR_SIZE  NUMERIC(6) = 4000;
  declare @LARGEST_VARBINARY_SIZE NUMERIC(6) = 8000;
  declare @LARGEST_CHAR_TYPE      VARCHAR(32) = concat('char(',@LARGEST_CHAR_SIZE,')');
  declare @LARGEST_NCHAR_TYPE     VARCHAR(32) = concat('nchar(',@LARGEST_NCHAR_SIZE,')');
  declare @LARGEST_VARCHAR_TYPE   VARCHAR(32) = concat('varchar(',@LARGEST_VARCHAR_SIZE,')');
  declare @LARGEST_NVARCHAR_TYPE  VARCHAR(32) = concat('nvarchar(',@LARGEST_NVARCHAR_SIZE,')');
  declare @LARGEST_VARBINARY_TYPE VARCHAR(32) = concat('varbinary(',@LARGEST_VARBINARY_SIZE,')');

  declare @MAX_VARCHAR_TYPE       VARCHAR(32) = 'varchar(max)';
  declare @MAX_NVARCHAR_TYPE      VARCHAR(32) = 'nvarchar(max)';
  declare @MAX_VARBINARY_TYPE     VARCHAR(32) = 'varbinary(max)';

																 
  declare @DOUBLE_PRECISION_TYPE  VARCHAR(32) = 'float(53)';
  declare @INTERVAL_TYPE          VARCHAR(32) = 'varchar(16)';
  declare @BFILE_TYPE             VARCHAR(32) = 'nvarchar(2048)';
  declare @ROWID_TYPE             VARCHAR(32) = 'varchar(18)';
  declare @ENUM_TYPE              VARCHAR(32) = 'nvarchar(255)';
  declare @PGSQL_MONEY_TYPE       VARCHAR(32) = 'numeric(21,2)';
  declare @PGSQL_NUMERIC_TYPE     VARCHAR(32) = 'numeric(38,19)';
  declare @PGSQL_NAME_TYPE        VARCHAR(32) = 'nvarchar(64)';
  declare @PGSQL_SINGLE_CHAR_TYPE VARCHAR(32) = 'nchar(1)';
  declare @INET_ADDR_TYPE         VARCHAR(32) = 'varchar(39)';
  declare @MAC_ADDR_TYPE          VARCHAR(32) = 'varchar(23)';
  declare @UNSIGNED_INT_TYPE      VARCHAR(32) = 'numeric(10,0)';
  declare @PGSQL_IDENTIFIER       VARCHAR(32) = 'binary(4)';
  declare @ORACLE_NUMERIC_TYPE    VARCHAR(32) = 'numeric(38,19)';
  declare @MONGO_OBJECT_ID        VARCHAR(32) = 'binary(12)';
  declare @MONGO_UNKNOWN_TYPE     VARCHAR(32) = 'nvarchar(2048)';
  declare @MONGO_REGEX_TYPE       VARCHAR(32) = 'nvarchar(2048)';
  
  return 
  case
    when ((@VENDOR = 'MSSQLSERVER') and (@DB_COLLATION like '%UTF8')) then
      case 
        when @DATA_TYPE = 'text'                                                                         then 'varchar(max)'
        when @DATA_TYPE = 'ntext'                                                                        then @MAX_NVARCHAR_TYPE 
                                                                                                         else lower(@DATA_TYPE)
      end      
    when @VENDOR = 'Oracle' then
      case 
        when @DATA_TYPE = 'VARCHAR2' 
         and @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE                                                  then @MAX_NVARCHAR_TYPE
        when @DATA_TYPE = 'VARCHAR2'                                                                     then 'nvarchar'
        when @DATA_TYPE = 'NVARCHAR2'                                                                    then 'nvarchar'
        when @DATA_TYPE = 'CLOB'                                                                         then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'NCLOB'                                                                        then @MAX_NVARCHAR_TYPE
        when @DATA_TYPE = 'NUMBER' 
         and @DATA_TYPE_LENGTH is NULL                                                                   then @ORACLE_NUMERIC_TYPE
        when @DATA_TYPE = 'NUMBER'                                                                       then 'decimal'
        when @DATA_TYPE = 'BINARY_DOUBLE'                                                                then @DOUBLE_PRECISION_TYPE
        when @DATA_TYPE = 'BINARY_FLOAT'                                                                 then 'real'
        when @DATA_TYPE = 'BOOLEAN'                                                                      then 'bit'
        when @DATA_TYPE = 'RAW' then
         case 
           when @DATA_TYPE_LENGTH = 1                                                                    then 'bit'
           when @DATA_TYPE_LENGTH > @LARGEST_VARBINARY_SIZE                                              then @MAX_VARBINARY_TYPE 
                                                                                                         else 'varbinary'
         end
        when @DATA_TYPE = 'BLOB'                                                                         then @MAX_VARBINARY_TYPE
        when @DATA_TYPE = 'BFILE'                                                                        then @BFILE_TYPE  
        when @DATA_TYPE in ('ROWID','UROWID')                                                            then @ROWID_TYPE                 
        when @DATA_TYPE in ('ANYDATA')                                                                   then @MAX_NVARCHAR_TYPE                 
        when (CHARINDEX('INTERVAL',@DATA_TYPE) = 1)                                                      then @INTERVAL_TYPE                  
        when (CHARINDEX('TIMESTAMP',@DATA_TYPE) = 1) then 
          case
            when (CHARINDEX('TIME ZONE',@DATA_TYPE) > 0)                                                 then 'datetimeoffset'
                                                                                                         else'datetime2' 
          end
        when @DATA_TYPE = 'XMLTYPE'                                                                      then 'xml'
        when @DATA_TYPE = '"MDSYS"."SDO_GEOMETRY"'                                                       then 'geometry'                
        when @DATA_TYPE like '"%"."%"'                                                                   then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'JSON'                                                                         then 'json'
                                                                                                         else lower(@DATA_TYPE)
      end
    when @VENDOR in ('MySQL','MariaDB') then
      case 
        when @DATA_TYPE = 'tinyint' 
         and @DATA_TYPE_LENGTH = 1                                                                       then 'bit'
        when @DATA_TYPE = 'boolean'                                                                      then 'bit'
        when @DATA_TYPE = 'mediumint'                                                                    then 'int'
        when @DATA_TYPE in (
          'numeric',
          'decimal'
        ) then
        case 
          when  @DATA_TYPE_LENGTH > 38
           and (@DATA_TYPE_SCALE = 0)                                                                    then 'numeric(38,0)'
          when (@DATA_TYPE_LENGTH > 38) 
           and (@DATA_TYPE_SCALE > 0)                                                                    then concat('numeric(38,',cast(round(@DATA_TYPE_SCALE*(38.0/@DATA_TYPE_LENGTH),0) as numeric(2)),')')
                                                                                                         else 'numeric'
        end
        when @DATA_TYPE = 'datetime'                                                                     then 'datetime2'
        when @DATA_TYPE = 'timestamp'                                                                    then 'datetime2'
        when @DATA_TYPE = 'float'                                                                        then 'real'
        when @DATA_TYPE = 'double'                                                                       then @DOUBLE_PRECISION_TYPE
        when @DATA_TYPE = 'enum'                                                                         then @ENUM_TYPE
        when @DATA_TYPE = 'set'                                                                          then 'json'
        when @DATA_TYPE = 'year'                                                                         then 'smallint'
        when @DATA_TYPE = 'json'                                                                         then 'json'
        when @DATA_TYPE = 'blob' then
          case 
            when @DATA_TYPE_LENGTH > @LARGEST_VARBINARY_SIZE                                                                then @MAX_VARBINARY_TYPE
                                                                                                         else 'varbinary'
        end
        -- For MySQL may need to add column character set to the table metadata object 
        -- in order to accutately determine varchar Vs nvarchar ? 
        -- Alternatively the character set could be used when generating metadata from 
        -- the MySQL dictionaly that distinguishes varchar from nvarchar even thought 
        -- the dictionaly does not.
        when @DATA_TYPE = 'varchar' then
         case
           when @DATA_TYPE_LENGTH is null                                                                then @MAX_NVARCHAR_TYPE 
           when @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE                                               then @MAX_NVARCHAR_TYPE 
                                                                                                         else 'nvarchar'
        end
        when @DATA_TYPE = 'text'                                                                         then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'mediumtext'                                                                   then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'longtext'                                                                     then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'longblob'                                                                     then @MAX_VARBINARY_TYPE
        when @DATA_TYPE = 'mediumblob'                                                                   then @MAX_VARBINARY_TYPE
        when @DATA_TYPE in (
          'point',
          'linestring',
          'polygon',
          'geometry',
          'multipoint',
          'multilinestring',
          'multipolygon',
          'geometrycollection',
          'geomcollection')                                                                              then 'geometry'
                                                                                                         else lower(@DATA_TYPE)
      end
    when @VENDOR = 'Postgres' then
      case 
        when @DATA_TYPE = 'character varying' then
         case  
           when @DATA_TYPE_LENGTH is null                                                                then @MAX_NVARCHAR_TYPE 
           when @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE                                               then @MAX_NVARCHAR_TYPE 
                                                                                                         else 'nvarchar'
        end                                                                                                              
        when @DATA_TYPE = 'character' then                                                                
         case  
           when @DATA_TYPE_LENGTH is null                                                                then @MAX_NVARCHAR_TYPE 
           when @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE                                               then @MAX_NVARCHAR_TYPE 
           when @DATA_TYPE_LENGTH > @LARGEST_NCHAR_SIZE                                                  then 'nvarchar'
                                                                                                         else 'nchar'
        end                                                                                                              
        when @DATA_TYPE = 'bpchar' then                                                                
         case  
           when @DATA_TYPE_LENGTH is null                                                                then @MAX_NVARCHAR_TYPE 
           when @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE                                               then @MAX_NVARCHAR_TYPE 
                                                                                                         else 'nchar'
        end                                                                                                              
        when @DATA_TYPE = 'text'                                                                         then @MAX_NVARCHAR_TYPE
	    when @DATA_TYPE = 'char'                                                                         then @PGSQL_SINGLE_CHAR_TYPE
        when @DATA_TYPE = 'name'                                                                         then @PGSQL_NAME_TYPE
																													 
        when @DATA_TYPE = 'bytea' then
         case  
           when @DATA_TYPE_LENGTH is null                                                                then @MAX_VARBINARY_TYPE 
           when @DATA_TYPE_LENGTH > @LARGEST_VARBINARY_SIZE                                              then @MAX_VARBINARY_TYPE 
                                                                                                         else 'varbinary'
        end                                                                                                              
        when @DATA_TYPE = 'integer'                                                                      then 'int'
        when @DATA_TYPE in (
          'numeric',
          'decimal'
        ) then
         case  
           when @DATA_TYPE_LENGTH is null                                                                then @PGSQL_NUMERIC_TYPE 
                                                                                                         else 'numeric'
        end                                                                                                              
        when @DATA_TYPE = 'money'                                                                        then @PGSQL_MONEY_TYPE
        when @DATA_TYPE = 'double precision'                                                             then @DOUBLE_PRECISION_TYPE
        when @DATA_TYPE = 'real'                                                                         then 'real'
        when @DATA_TYPE = 'boolean'                                                                      then 'bit'
        when @DATA_TYPE = 'timestamp'                                                                    then 'datetime'
        when @DATA_TYPE = 'timestamp with time zone'                                                     then 'datetimeoffset'
        when @DATA_TYPE = 'timestamp without time zone'                                                  then 'datetime2'
        when @DATA_TYPE = 'time without time zone'                                                       then 'time'
        when @DATA_TYPE = 'time with time zone'                                                          then 'datetimeoffset'
        when (CHARINDEX('interval',@DATA_TYPE) = 1)                                                      then @INTERVAL_TYPE
        when @DATA_TYPE = 'jsonb'                                                                        then 'json'
        when @DATA_TYPE = 'geometry'                                                                     then 'geometry'
        when @DATA_TYPE = 'geography'                                                                    then 'geography'
        when @DATA_TYPE = 'circle' 
         and @CIRCLE_FORMAT = 'CIRCLE'                                                                   then 'json'
        when @DATA_TYPE in (
          'point',
          'lseg',
          'box',
          'path',
          'polygon',
          'circle'
        )                                                                                                then 'geometry'
        when @DATA_TYPE = 'line'                                                                         then 'json'
        when @DATA_TYPE = 'uuid'                                                                         then 'uniqueidentifier'
        when @DATA_TYPE in (
          'bit',
          'bit varying'
        ) then 
        case  
          when (@DATA_TYPE_LENGTH is null)                                                               then @LARGEST_VARCHAR_TYPE 
          when (@DATA_TYPE_LENGTH > @LARGEST_VARBINARY_SIZE)                                             then @MAX_VARCHAR_TYPE 
                                                                                                         else 'varchar'
        end
        when @DATA_TYPE in (
          -- IPv4 or IPv6 internet address
          'cidr',
          'inet'
        )                                                                                                then @INET_ADDR_TYPE
        when @DATA_TYPE in (
          -- Mac Address
          'macaddr',
          'macaddr8'
         )                                                                                               then @MAC_ADDR_TYPE
        when @DATA_TYPE in (
          -- Range Types: Map to JSON
          'int4range',
          'int8range',
          'numrange',
          'tsrange',
          'tstzrange',
          'daterange'
        )                                                                                                then 'json'
        when @DATA_TYPE in (
          -- Representation of a Text Search
          -- Sorted list of distinct lexemes used by Text Search.
          -- GiST Index
          'tsvector',
          'gtsvector'
        )                                                                                                then 'json'
        when @DATA_TYPE in ('tsquery')                                                                   then @MAX_NVARCHAR_TYPE
        when @DATA_TYPE in (
          -- Postgres Object Identifier. Per documentation unsigned 4 Byte Integer 
          'oid',
          'regcollation',
          'regclass',
          'regconfig',
          'regdictionary',
          'regnamespace',
          'regoper',
          'regoperator',
          'regproc',
          'regprocedure',
          'regrole',
          'regtype'
        )                                                                                                then @UNSIGNED_INT_TYPE
        when @DATA_TYPE in (
          -- Tranasaction Identifiers 32 bits.
          'tid',
          'xid',
          'cid',
          'txid_snapshot'
        )                                                                                                then @PGSQL_IDENTIFIER
        when @DATA_TYPE in (
          -- Postgres ACLItem & REF Cursor. Map to JSON 
          'aclitem',
          'refcursor'
        )                                                                                                then 'json'
                                                                                                         else lower(@DATA_TYPE)
      end          
    when @VENDOR = 'MongoDB' then
      -- MongoDB typing based on BSON type model 
      case
        when @DATA_TYPE = 'string' 
         and (@DATA_TYPE_LENGTH is null or @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE)                   then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'string'                                                                       then 'nvarchar'
        when @DATA_TYPE = 'int'                                                                          then 'int'
        when @DATA_TYPE = 'long'                                                                         then 'bigint'
        when @DATA_TYPE = 'double'                                                                       then @DOUBLE_PRECISION_TYPE
        when @DATA_TYPE = 'decimal'                                                                      then 'numeric(38,19)'
        when @DATA_TYPE = 'binData'                                                                      then @MAX_VARBINARY_TYPE
        when @DATA_TYPE = 'bool'                                                                         then 'bit'
        when @DATA_TYPE = 'date'                                                                         then 'datatime2'
        when @DATA_TYPE = 'timestamp'                                                                    then 'datatime2'
        when @DATA_TYPE = 'objectId'                                                                     then @MONGO_OBJECT_ID
        when @DATA_TYPE = 'object'                                                                       then 'json'          
        when @DATA_TYPE = 'array'                                                                        then 'json'
        when @DATA_TYPE = 'null'                                                                         then @MONGO_UNKNOWN_TYPE
        when @DATA_TYPE = 'regex'                                                                        then @MONGO_REGEX_TYPE
        when @DATA_TYPE = 'javascript'                                                                   then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'javascriptWithScope'                                                          then @MAX_NVARCHAR_TYPE          
        when @DATA_TYPE = 'minkey'                                                                       then 'json'          
        when @DATA_TYPE = 'maxkey'                                                                       then 'json'          
        when @DATA_TYPE in (
          'undefined',
          'dbPointer',
          'function',
          'symbol'
        )                                                                                                then 'json'
                                                                                                         else lower(@DATA_TYPE)
      end
    when @VENDOR = 'SNOWFLAKE' then
      case
        when @DATA_TYPE = 'TEXT' 
         and (@DATA_TYPE_LENGTH is null or @DATA_TYPE_LENGTH > @LARGEST_NVARCHAR_SIZE)                   then @MAX_NVARCHAR_TYPE 
        when @DATA_TYPE = 'TEXT'                                                                         then 'nvarchar'
        when @DATA_TYPE = 'BINARY' 
         and (@DATA_TYPE_LENGTH is null or @DATA_TYPE_LENGTH > @LARGEST_VARBINARY_SIZE)                  then @MAX_VARBINARY_TYPE
																														 
        when @DATA_TYPE = 'FLOAT'                                                                        then @DOUBLE_PRECISION_TYPE
        when @DATA_TYPE = 'BINARY'                                                                       then 'varbinary'
        when @DATA_TYPE = 'BOOLEAN'                                                                      then 'bit'
        when @DATA_TYPE = 'VARIANT'                                                                      then @MAX_VARBINARY_TYPE
        when @DATA_TYPE = 'TIMESTAMP_NTZ' 
         and (@DATA_TYPE_LENGTH > 7)                                                                     then 'datetimeoffset(7)'
        when @DATA_TYPE = 'TIMESTAMP_NTZ'                                                                then 'datetimeoffset'
        when @DATA_TYPE = 'TIMESTAMP_LTZ' 
         and (@DATA_TYPE_LENGTH > 7)                                                                     then 'datetime2(7)'
        when @DATA_TYPE = 'TIMESTAMP_LTZ'                                                                then 'datetime2'
        when @DATA_TYPE = 'NUMBER'                                                                       then 'decimal'
                                                                                                         else lower(@DATA_TYPE)
      end
    else 
      lower(@DATA_TYPE)
  end
end
--
go
--
EXECUTE sp_ms_marksystemobject 'sp_MAP_FOREIGN_DATATYPE'
go
--
IF OBJECT_ID('sp_GENERATE_STATEMENTS') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_GENERATE_STATEMENTS(@VENDOR NVARCHAR(128), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_NAMES_ARRAY XML, @DATA_TYPES_ARARY XML, @SIZE_CONSTRAINTS_ARRAY XML, @SPATIAL_FORMAT NVARCHAR(128), @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
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
ALTER FUNCTION sp_GENERATE_STATEMENTS(@VENDOR NVARCHAR(128), @SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_NAMES_ARRAY XML, @DATA_TYPES_ARARY XML, @SIZE_CONSTRAINTS_ARRAY XML, @SPATIAL_FORMAT NVARCHAR(128), @CIRCLE_FORMAT NVARCHAR(7), @DB_COLLATION NVARCHAR(32)) 
returns NVARCHAR(MAX)
as
begin
  DECLARE @COLUMN_LIST        NVARCHAR(MAX);  
  DECLARE @COLUMNS_CLAUSE     NVARCHAR(MAX);
  DECLARE @TARGET_DATA_TYPES  NVARCHAR(MAX)
  
  DECLARE @DDL_STATEMENT      NVARCHAR(MAX);
  DECLARE @DML_STATEMENT      NVARCHAR(MAX);
  DECLARE @LEGACY_COLLATION   VARCHAR(256);
  
  SET @LEGACY_COLLATION = @DB_COLLATION;
  
  if (@LEGACY_COLLATION like '%_UTF8') begin
    SET @LEGACY_COLLATION = LEFT(@LEGACY_COLLATION, LEN(@LEGACY_COLLATION)-5);
  end;
 
  if (@LEGACY_COLLATION like '%_SC') begin
    SET @LEGACY_COLLATION = LEFT(@LEGACY_COLLATION, LEN(@LEGACY_COLLATION)-3);
  end;
  
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
                      end 
                      as BIGINT) "DATA_TYPE_LENGTH"
                ,case
                   when CHARINDEX(',',s."VALUE") > 0  then 
                     RIGHT(s."VALUE", CHARINDEX(',',REVERSE(s."VALUE"))-1)
                   else 
                     NULL
                 end "DATA_TYPE_SCALE"
            FROM ( select T."COLUMN_NAME".value('.', 'NVARCHAR(128)') as "VALUE", row_number() over(order by T.COLUMN_NAME) as "KEY" from @COLUMN_NAMES_ARRAY.nodes('/columnNames/columnName') as T("COLUMN_NAME")) c,
			     ( select T."DATA_TYPE".value('.', 'NVARCHAR(128)') as "VALUE", row_number() over(order by T.DATA_TYPE) as "KEY" from @DATA_TYPES_ARARY.nodes('/dataTypes/dataType') as T("DATA_TYPE")) t,
			     ( select T."SIZE_CONSTRAINT".value('.', 'NVARCHAR(128)') as "VALUE", row_number() over(order by T.SIZE_CONSTRAINT) as "KEY" from @SIZE_CONSTRAINTS_ARRAY.nodes('/sizeConstraints/sizeConstraint') as T("SIZE_CONSTRAINT")) s
           WHERE c."KEY" = t."KEY" and c."KEY" = s."KEY"
  ),
  "TARGET_TABLE_DEFINITION" as (
    select st.*, master.dbo.sp_MAP_FOREIGN_DATATYPE(@VENDOR, "DATA_TYPE", "DATA_TYPE_LENGTH", "DATA_TYPE_SCALE", @CIRCLE_FORMAT, @DB_COLLATION) TARGET_DATA_TYPE
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
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_STATEMENTS'
go
--
IF OBJECT_ID('sp_GENERATE_SQL') IS NOT NULL
   set noexec on
go
--
CREATE FUNCTION sp_GENERATE_SQL(@TARGET_DATABASE VARCHAR(128), @SPATIAL_FORMAT NVARCHAR(128), @METADATA NVARCHAR(MAX),@DB_COLLATION NVARCHAR(32)) 
returns NVARCHAR(MAX)
as
begin
  return NULL
end
GO
--
set noexec off
go

ALTER FUNCTION sp_GENERATE_SQL(@TARGET_DATABASE VARCHAR(128), @SPATIAL_FORMAT NVARCHAR(128), @METADATA NVARCHAR(MAX),@DB_COLLATION NVARCHAR(32)) 
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
         master.dbo.sp_GENERATE_STATEMENTS(VENDOR, @TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_NAMES_ARRAY, v.DATA_TYPES_ARARY, v.SIZE_CONSTRAINTS_ARRAY, @SPATIAL_FORMAT, @CIRCLE_FORMAT, @DB_COLLATION) as STATEMENTS
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
EXECUTE sp_ms_marksystemobject 'sp_GENERATE_SQL'
go
--
EXIT