/*
**
** MySQL YADAMU_IMPORT Function.
**
*/
SET SESSION SQL_MODE=ANSI_QUOTES;
--
DROP FUNCTION IF EXISTS MAP_FOREIGN_DATATYPE;
--
DELIMITER $$
--
CREATE FUNCTION MAP_FOREIGN_DATATYPE(P_SOURCE_VENDOR VARCHAR(128), P_DATA_TYPE VARCHAR(128), P_DATA_TYPE_LENGTH INT, P_DATA_TYPE_SIZE INT, P_CIRCLE_FORMAT VARCHAR(7)) 
RETURNS CHAR(128) DETERMINISTIC
BEGIN

  DECLARE C_TINYTEXT_SIZE           SMALLINT DEFAULT 255;
  DECLARE C_TEXT_SIZE                    INT DEFAULT 65535;
  DECLARE C_MEDIUMTEXT_SIZE              INT DEFAULT 16777215;
  DECLARE C_LONGTEXT_SIZE             BIGINT DEFAULT 4294967295;

  DECLARE C_TINYBLOB_SIZE           SMALLINT DEFAULT 255;
  DECLARE C_BLOB_SIZE                    INT DEFAULT 65535;
  DECLARE C_MEDIUMBLOB_SIZE              INT DEFAULT 16777215;
  DECLARE C_LONGBLOB_SIZE             BIGINT DEFAULT 4294967295;

  DECLARE C_LARGEST_CHAR_SIZE       SMALLINT DEFAULT 255;
  DECLARE C_LARGEST_BINARY_SIZE     SMALLINT DEFAULT 255;
  DECLARE C_LARGEST_VARCHAR_SIZE    SMALLINT DEFAULT 4096;
  DECLARE C_LARGEST_VARBINARY_SIZE  SMALLINT DEFAULT 8192;
  
  DECLARE C_LARGEST_CHAR_TYPE       VARCHAR(32) DEFAULT concat('char(',C_LARGEST_CHAR_SIZE,')');
  DECLARE C_LARGEST_BINARY_TYPE     VARCHAR(32) DEFAULT concat('binary(',C_LARGEST_BINARY_SIZE,')');
  DECLARE C_LARGEST_VARCHAR_TYPE    VARCHAR(32) DEFAULT concat('varchar(',C_LARGEST_VARCHAR_SIZE,')');
  DECLARE C_LARGEST_VABRINARY_TYPE  VARCHAR(32) DEFAULT concat('varbinary(',C_LARGEST_VARBINARY_SIZE,')');
  /*
  **
  ** MySQL BIT Column truncates leading '0's 
  **
  
  DECLARE C_LARGEST_BIT_TYPE        VARCHAR(32) DEFAULT 'varchar(64)';
  DECLARE C_BIT_TYPE                VARCHAR(32) DEFAULT 'varchar';
  
  **
  */

  DECLARE C_LARGEST_BIT_TYPE        VARCHAR(32) DEFAULT 'bit(64)';
  DECLARE C_BIT_TYPE                VARCHAR(32) DEFAULT 'bit';
  

  DECLARE C_BFILE_TYPE              VARCHAR(32) DEFAULT 'varchar(2048)';
  DECLARE C_ROWID_TYPE              VARCHAR(32) DEFAULT 'varchar(32)';
  DECLARE C_XML_TYPE                VARCHAR(32) DEFAULT 'longtext';
  DECLARE C_UUID_TYPE               VARCHAR(32) DEFAULT 'varchar(36)';
  DECLARE C_ENUM_TYPE               VARCHAR(32) DEFAULT 'varchar(255)';
  DECLARE C_INTERVAL_TYPE           VARCHAR(32) DEFAULT 'varchar(16)';
  DECLARE C_BOOLEAN_TYPE            VARCHAR(32) DEFAULT 'tinyint(1)';
  DECLARE C_HIERARCHY_TYPE          VARCHAR(32) DEFAULT 'varchar(4000)';
  DECLARE C_MSSQL_MONEY_TYPE        VARCHAR(32) DEFAULT 'decimal(19,4)';
  DECLARE C_MSSQL_SMALL_MONEY_TYPE  VARCHAR(32) DEFAULT 'decimal(10,4)';
  DECLARE C_MSSQL_ROWVERSION_TYPE   VARCHAR(32) DEFAULT 'binary(8)';
  DECLARE C_PGSQL_MONEY_TYPE        VARCHAR(32) DEFAULT 'decimal(21,2)';
  DECLARE C_PGSQL_NAME_TYPE         VARCHAR(32) DEFAULT 'varchar(64)';
  DECLARE C_PGSQL_SINGLE_CHAR_TYPE  VARCHAR(32) DEFAULT 'char(1)';
  DECLARE C_INET_ADDR_TYPE          VARCHAR(32) DEFAULT 'varchar(39)';
  DECLARE C_MAC_ADDR_TYPE           VARCHAR(32) DEFAULT 'varchar(23)';
  DECLARE C_PGSQL_IDENTIFIER        VARCHAR(32) DEFAULT 'int unsigned';
  DECLARE C_PGSQL_NUMERIC           VARCHAR(32) DEFAULT 'decimal(65,30)';
  DECLARE C_ORACLE_NUMERIC          VARCHAR(32) DEFAULT 'decimal(65,30)';
  DECLARE C_MONGO_OBJECT_ID         VARCHAR(32) DEFAULT 'binary(12)';
  DECLARE C_MONGO_UNKNOWN_TYPE      VARCHAR(32) DEFAULT 'varchar(2048)';
  DECLARE C_MONGO_REGEX_TYPE        VARCHAR(32) DEFAULT 'varchar(2048)';
  
  case 
    when P_SOURCE_VENDOR = 'Oracle' then
      case 
        -- Oracle Mappings
        when P_DATA_TYPE = 'VARCHAR2'                           then return 'varchar';
        when P_DATA_TYPE = 'NVARCHAR2'                          then return 'varchar';
        when P_DATA_TYPE = 'NUMBER'                             then return case when P_DATA_TYPE_LENGTH is NULL then C_ORACLE_NUMERIC else 'decimal' end;
        when P_DATA_TYPE = 'BINARY_FLOAT'                       then return 'float';
        when P_DATA_TYPE = 'BINARY_DOUBLE'                      then return 'double';
        when P_DATA_TYPE = 'CLOB'                               then return 'longtext';
        when P_DATA_TYPE = 'BLOB'                               then return 'longblob';
        when P_DATA_TYPE = 'NCLOB'                              then return 'longtext';
        when P_DATA_TYPE = 'XMLTYPE'                            then return C_XML_TYPE;
        when P_DATA_TYPE = 'TIMESTAMP'                          then return case when P_DATA_TYPE_LENGTH > 6 then 'datetime(6)' else 'datetime' end;
        when P_DATA_TYPE = 'BFILE'                              then return C_BFILE_TYPE;
        when P_DATA_TYPE = 'ROWID'                              then return C_ROWID_TYPE;
        when P_DATA_TYPE = 'RAW'                                then return 'varbinary';
        when P_DATA_TYPE = 'ANYDATA'                            then return 'longtext';
        when P_DATA_TYPE = '"MDSYS"."SDO_GEOMETRY"'             then return 'geometry';
        when (instr(P_DATA_TYPE,'TIME ZONE') > 0)               then return 'datetime'; 
        when (instr(P_DATA_TYPE,'INTERVAL') = 1)                then return C_INTERVAL_TYPE;
        when (instr(P_DATA_TYPE,'XMLTYPE') > 0)                 then return C_XML_TYPE;
        when (instr(P_DATA_TYPE,'"."') > 0)                     then return 'longtext';
		                                                        else return lower(P_DATA_TYPE);
      end case;                                                 
    when P_SOURCE_VENDOR = 'MSSQLSERVER' then                   
      case                                                      
        -- SQLServer Mapppings                                  
        when P_DATA_TYPE in (
		  'varchar',
		  'nvarchar'                           
		) then
        case		
		  when P_DATA_TYPE_LENGTH = -1                          then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARCHAR_SIZE      then return 'text';
                                                                else return 'varchar'; 
	    end case;
        when P_DATA_TYPE IN (
  		  'char',
          'nchar'
        ) then	  
        case		
		  when P_DATA_TYPE_LENGTH = -1                          then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_CHAR_SIZE         then return 'text';
                                                                else return 'char';
        end case;
        when P_DATA_TYPE = 'text'                               then return 'longtext';
        when P_DATA_TYPE = 'ntext'                              then return 'longtext';
        when P_DATA_TYPE = 'binary' then                           
		case
		  when P_DATA_TYPE_LENGTH = -1                          then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_SIZE           then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_BLOB_SIZE                 then return 'mediumblob';
          when P_DATA_TYPE_LENGTH > C_LARGEST_BINARY_SIZE       then return 'blob';
                                                                else return 'binary';
        end case;
        when P_DATA_TYPE = 'varbinary' then                            
		case
		  when P_DATA_TYPE_LENGTH = -1                          then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_SIZE           then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_BLOB_SIZE                 then return 'mediumblob';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARBINARY_SIZE    then return 'blob';
                                                                else return 'varbinary';
		end case;														           
        when P_DATA_TYPE = 'tinyint'                            then return 'smallint';
        when P_DATA_TYPE = 'mediumint'                          then return 'int';
        when P_DATA_TYPE = 'smallmoney'                         then return C_MSSQL_SMALL_MONEY_TYPE;
        when P_DATA_TYPE = 'money'                              then return C_MSSQL_MONEY_TYPE;
        when P_DATA_TYPE = 'real'                               then return 'float';
        when P_DATA_TYPE = 'bit'                                then return C_BOOLEAN_TYPE;
        when P_DATA_TYPE = 'image'                              then return 'longblob';
        when P_DATA_TYPE = 'datetime'                           then return 'datetime(3)';
        when P_DATA_TYPE = 'datetime2'                          then return case when P_DATA_TYPE_LENGTH > 6 then 'datetime(6)' else 'datetime'end;
        when P_DATA_TYPE = 'time'                               then return case when P_DATA_TYPE_LENGTH > 6 then 'time(6)' else 'time'end;
        when P_DATA_TYPE = 'datetimeoffset'                     then return case when P_DATA_TYPE_LENGTH > 6 then 'datetime(6)' else 'datetime'end;
        when P_DATA_TYPE = 'smalldate'                          then return 'datetime';
        when P_DATA_TYPE = 'geography'                          then return 'geometry';
        when P_DATA_TYPE = 'geometry'                           then return 'geometry';
        when P_DATA_TYPE = 'uniqueidentifier'                   then return C_UUID_TYPE;
        when P_DATA_TYPE = 'xml'                                then return C_XML_TYPE;
        when P_DATA_TYPE = 'hierarchyid'                        then return C_HIERARCHY_TYPE;
        when P_DATA_TYPE = 'rowversion'                         then return C_MSSQL_ROWVERSION_TYPE;
                                                                else return lower(P_DATA_TYPE);
      end case;                                                 
    when P_SOURCE_VENDOR = 'Postgres' then                      
      case                                                      
        -- Postgres Mapppings                                   
        when P_DATA_TYPE = 'character varying' then                 
        case 
		  when P_DATA_TYPE_LENGTH is null                       then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARCHAR_SIZE      then return 'text';
                                                                else return 'varchar';
        end case;
        when P_DATA_TYPE = 'character' then                         
        case 
		  when P_DATA_TYPE_LENGTH is null                       then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_CHAR_SIZE         then return 'text';
                                                                else return 'char';
        end case;
        when P_DATA_TYPE = 'text'                               then return 'longtext';
	    when P_DATA_TYPE = 'char'                               then return C_PGSQL_SINGLE_CHAR_TYPE;
	    when P_DATA_TYPE = 'name'                               then return C_PGSQL_NAME_TYPE;
        when P_DATA_TYPE = 'bpchar' then                         
        case 
		  when P_DATA_TYPE_LENGTH is null                       then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_CHAR_SIZE         then return 'text';
                                                                else return 'char';
	    end case;
        when P_DATA_TYPE = 'bytea' then                            
        case 
		  when P_DATA_TYPE_LENGTH is null                       then return C_LARGEST_VABRINARY_TYPE;
          when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_SIZE           then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_BLOB_SIZE                 then return 'mediumblob';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARBINARY_SIZE    then return 'blob';
                                                                else return 'varbinary';
	    end case;
        when P_DATA_TYPE in (
		  'numeric',
		  'decimal'
		) then                                                    
		case 
		  when P_DATA_TYPE_LENGTH is NULL                       then return C_PGSQL_NUMERIC; 
		                                                        else return 'decimal';
	    end case;
        when P_DATA_TYPE = 'integer'                            then return 'int';
        when P_DATA_TYPE = 'double precision'                   then return 'double';
        when P_DATA_TYPE = 'real'                               then return 'float';
        when P_DATA_TYPE = 'money'                              then return C_PGSQL_MONEY_TYPE;
        when P_DATA_TYPE = 'boolean'                            then return C_BOOLEAN_TYPE;

		/*
		**
		** There appears to be 100% impedance mismatch
		** betweem Postges and MySQL BIT data types
		**
		*/

        when P_DATA_TYPE in (
		  'bit',
		  'bit varying'
		) then                                                      
	    case 
		  -- when P_DATA_TYPE_LENGTH is null                    then return C_LARGEST_BIT_TYPE;
          when P_DATA_TYPE_LENGTH is null                       then return C_LARGEST_VARCHAR_TYPE;
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARCHAR_SIZE      then return 'text';
		  when P_DATA_TYPE_LENGTH > 64                          then return 'varchar';
          --                                                    else return C_BIT_TYPE;
                                                                else return 'varchar';
        end case;														    
        when P_DATA_TYPE like ('timestamp %')                   then return case when P_DATA_TYPE_LENGTH is NULL then 'datetime(6)' else  'datetime' end;
        when P_DATA_TYPE like ('time %')                        then return case when P_DATA_TYPE_LENGTH is NULL then 'time(6)' else  'time' end;
        when P_DATA_TYPE like 'interval%'                       then return C_INTERVAL_TYPE;
        when P_DATA_TYPE = 'xml'                                then return C_XML_TYPE;
        when P_DATA_TYPE = 'jsonb'                              then return 'json';
        when P_DATA_TYPE in (
		 'geography',
		 'geometry'
		)                                                       then return 'geometry';
        when P_DATA_TYPE = 'point'                              then return 'point';
        when P_DATA_TYPE in (
		  'lseg',
		  'path'
		)                                                       then return 'linestring';
   	    when P_DATA_TYPE = 'circle'                             then return case when  P_CIRCLE_FORMAT = 'CIRCLE'  then 'json' else  'polygon' end;
		when P_DATA_TYPE in (
		  'box',
		  'polygon'
		)                                                       then return 'polygon';
        when P_DATA_TYPE = 'line'                               then return 'json';		 
        when P_DATA_TYPE = 'uuid'                               then return C_UUID_TYPE;
        when P_DATA_TYPE in (
		-- IPv4 or IPv6 internet address
          'cidr',
		  'inet'
		)                                                       then return C_INET_ADDR_TYPE;
        when P_DATA_TYPE in (
		  -- Mac Address
          'macaddr',
		  'macaddr8'
		)                                                       then return C_MAC_ADDR_TYPE;
		when P_DATA_TYPE in (
		-- Range Types: Map to JSON
		  'int4range',
		  'int8range',
		  'numrange',
		  'tsrange',
		  'tstzrange',
		  'daterange'
		)                                                       then return 'json';
		-- Sorted list of distinct lexemes used by Text Search.
		-- GiST Index
		when P_DATA_TYPE in (
		  'tsvector',
		  'gtsvector'
		)                                                       then return 'json';
		-- Representation of a Text Search
		when P_DATA_TYPE in ('tsquery')                         then return C_LARGEST_VARCHAR_TYPE;
		when P_DATA_TYPE in (
  		  -- Postgres Object Identifier.
		  -- Per documentation unsigned 4 Byte Interger 
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
		 )                                                      then return 'int unsigned';
		-- Tranasaction Identifiers 32 bits.
		when P_DATA_TYPE in (
		-- Tranasaction Identifiers 32 bits.
		   'tid',
		   'xid',
		   'cid',
		   'txid_snapshot'
		 )                                                      then return C_PGSQL_IDENTIFIER;
		when P_DATA_TYPE in (
		-- Postgres ACLItem & REF Cursor. Map to JSON 
		  'aclitem',
		  'refcursor'
		 )                                                      then return 'json';
                                                                else return lower(P_DATA_TYPE);
     end case;
    when P_SOURCE_VENDOR in ('MySQL','MariaDB') then
      case 
        -- Metadata does not contain sufficinet infromation 
		-- to rebuild ENUM and SET data types.
        when P_DATA_TYPE = 'boolean'                            then return C_BOOLEAN_TYPE;
        when P_DATA_TYPE = 'enum'                               then return C_ENUM_TYPE;
        when P_DATA_TYPE = 'set'                                then return 'json';
                                                                else return lower(P_DATA_TYPE);
      end case;       
    when P_SOURCE_VENDOR = 'Vertica' then
      case 
        -- Metadata does not contain sufficinet infromation 
		-- to rebuild ENUM and SET data types.
        when P_DATA_TYPE = 'char' then
          case		
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_CHAR_SIZE         then return 'text';
                                                                else return 'char';
	    end case;
        when P_DATA_TYPE = 'long varchar' then
          case		
            when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE         then return 'longtext';
            when P_DATA_TYPE_LENGTH > C_TEXT_SIZE               then return 'mediumtext';
            when P_DATA_TYPE_LENGTH > C_LARGEST_VARCHAR_SIZE    then return 'text';
                                                                else return 'varchar'; 
	    end case;
        when P_DATA_TYPE = 'long varbinary' then 
        case 
          when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_SIZE           then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_BLOB_SIZE                 then return 'mediumblob';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARBINARY_SIZE    then return 'blob';
                                                                else return 'varbinary';
	    end case;
        when P_DATA_TYPE = 'float'                              then return 'double';
        when P_DATA_TYPE = 'time'                               then return case when P_DATA_TYPE_LENGTH is NULL then 'time(6)' else  'time' end;
        when P_DATA_TYPE = 'timetz'                             then return 'datetime(6)';
        when P_DATA_TYPE = 'timestamptz'                        then return 'datetime(6)';
        when P_DATA_TYPE = 'timestamp'                          then return 'datetime(6)';
        when (instr(P_DATA_TYPE,'INTERVAL') = 1)                then return C_INTERVAL_TYPE;
        when P_DATA_TYPE = 'xml'                                then return C_XML_TYPE;
	    when P_DATA_TYPE = 'uuid'                               then return C_UUID_TYPE;
        when P_DATA_TYPE in (
		 'geography',
		 'geometry'
		)                                                       then return 'geometry';
                                                                else return lower(P_DATA_TYPE);
      end case;       
    when P_SOURCE_VENDOR = 'MongoDB' then
      -- MongoDB typing based on BSON type model
      case
        when P_DATA_TYPE = 'string' then                            
		case 
		  when P_DATA_TYPE_LENGTH is null                       then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
                                                                else return 'varchar';           
        end case;																    
        when P_DATA_TYPE = 'int'                                then return 'int';
        when P_DATA_TYPE = 'long'                               then return 'bigint';
        when P_DATA_TYPE = 'double'                             then return 'double';
        when P_DATA_TYPE = 'decimal'                            then return 'decimal(65,30)';
        when P_DATA_TYPE = 'binData'                            then return 'longblob';
        when P_DATA_TYPE = 'bool'                               then return C_BOOLEAN_TYPE;
        when P_DATA_TYPE = 'date'                               then return 'datetime(6)';
        when P_DATA_TYPE = 'timestamp'                          then return 'datetime(6)';
        when P_DATA_TYPE = 'ObjectId'                           then return C_MONGO_OBJECT_ID;
        when P_DATA_TYPE = 'object'                             then return 'JSON';
        when P_DATA_TYPE = 'array'                              then return 'JSON';
        when P_DATA_TYPE = 'null'                               then return C_MONGO_UNKNOWN_TYPE;
        when P_DATA_TYPE = 'regex'                              then return C_MONGO_REGEX_TYPE;
        when P_DATA_TYPE = 'javascript'                         then return 'longtext';
        when P_DATA_TYPE = 'javascriptWithScope'                then return 'longtext';
        when P_DATA_TYPE = 'minkey'                             then return 'JSON';
        when P_DATA_TYPE = 'maxkey'                             then return 'JSON';
        when P_DATA_TYPE in (
		  'undefined',
		  'dbPointer',
		  'function',
		  'symbol'
		)                                                       then return 'JSON';
                                                                else return lower(P_DATA_TYPE);
      end case;    
    when P_SOURCE_VENDOR = 'SNOWFLAKE' then
      case
        when P_DATA_TYPE = 'text' then                              
		case 
          when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_SIZE           then return 'longtext';
          when P_DATA_TYPE_LENGTH > C_TEXT_SIZE                 then return 'mediumtext';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARCHAR_SIZE      then return 'text';
                                                                else return 'varchar';
        end case;
        when P_DATA_TYPE = 'binary' then                            
		case
          when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_SIZE           then return 'longblob';
          when P_DATA_TYPE_LENGTH > C_BLOB_SIZE                 then return 'mediumblob';
          when P_DATA_TYPE_LENGTH > C_LARGEST_VARBINARY_SIZE    then return 'blob';
 		                                                        else return 'varbinary';
        end case;
        when P_DATA_TYPE = 'number'                             then return 'decimal';
        when P_DATA_TYPE = 'float'                              then return 'double';
        when P_DATA_TYPE = 'geography'                          then return 'geometry';
        when P_DATA_TYPE = 'time'                       		then return case when P_DATA_TYPE_LENGTH > 6 then 'time(6)' else  'time' end;
        when P_DATA_TYPE like ('timestamp_%')                   then return case when P_DATA_TYPE_LENGTH > 6 then 'datetime(6)' else  'datetime' end;
        when P_DATA_TYPE = 'xml'                                then return C_XML_TYPE;
        when P_DATA_TYPE = 'variant'                            then return 'longblob';
                                                                else return lower(P_DATA_TYPE);
       end case;
	else
      return lower(P_DATA_TYPE);
  end case;
end;
$$
--
DELIMITER ;
--
DROP PROCEDURE IF EXISTS GENERATE_SQL;
--
DELIMITER $$
--
CREATE PROCEDURE GENERATE_SQL(P_SOURCE_VENDOR VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128), P_TABLE_NAME VARCHAR(128), P_COLUMN_NAME_ARRAY JSON, P_DATA_TYPE_ARRAY JSON, P_SIZE_CONSTRAINT_ARRAY JSON, P_SPATIAL_FORMAT VARCHAR(7), P_CIRCLE_FORMAT VARCHAR(7), OUT P_TABLE_INFO JSON)
BEGIN
  DECLARE V_COLUMN_LIST        TEXT;
  DECLARE V_COLUMNS_CLAUSE     TEXT;
  DECLARE V_INSERT_SELECT_LIST TEXT;
  DECLARE V_COLUMN_PATTERNS    TEXT;
  DECLARE V_DML_STATEMENT      TEXT;
  DECLARE V_DDL_STATEMENT      TEXT;
  DECLARE V_TARGET_DATA_TYPES  TEXT;
  DECLARE V_ACTUAL_DATA_TYPES  TEXT;

  DECLARE V_COLUMN_COUNT      INT;
  DECLARE V_SQLSTATE          INT;
  DECLARE V_SQLERRM           TEXT;
  
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 

  select P_SOURCE_VENDOR, P_TARGET_SCHEMA, P_TABLE_NAME, P_COLUMN_NAME_ARRAY, P_DATA_TYPE_ARRAY, P_SIZE_CONSTRAINT_ARRAY, P_SPATIAL_FORMAT, P_CIRCLE_FORMAT;
 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, 
        V_SQLERRM = MESSAGE_TEXT;
        
        SET P_TABLE_INFO = JSON_OBJECT('ddl',V_DDL_STATEMENT,'dml',V_DML_STATEMENT,'targetDataTypes',CAST(V_TARGET_DATA_TYPES as JSON),
                                       'error', JSON_OBJECT(
                                                  'severity',         'FATAL',
                                                  'tableName',        P_TABLE_NAME,
                                                  'columnNames',      P_COLUMN_NAME_ARRAY, 
                                                  'dataTypes',        P_DATA_TYPE_ARRAY,
                                                  'sizeConstraints',  P_SIZE_CONSTRAINT_ARRAY,
                                                  'code',             V_SQLSTATE, 
                                                  'msg',              V_SQLERRM, 
                                                  'details',          'GENERATE_SQL' 
                                                )
                                      );

  END;  

  with 
    "SOURCE_TABLE_DEFINITIONS" 
    as ( 
      select c."KEY" IDX
            ,c.VALUE "COLUMN_NAME"
            ,t.VALUE "DATA_TYPE"
            ,case
               when s.VALUE in ('','null') then
                 NULL
               when INSTR(s.VALUE,',') > 0 then
                 SUBSTR(s.VALUE,1,INSTR(s.VALUE,',')-1)
               else
                 s.VALUE
             end "DATA_TYPE_LENGTH"
            ,case
               when INSTR(s.VALUE,',') > 0 then
                 SUBSTR(s.VALUE, INSTR(s.VALUE,',')+1)
               else
                 NULL
             end "DATA_TYPE_SCALE"
           from JSON_TABLE(P_COLUMN_NAME_ARRAY,     '$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(128) PATH '$')) c
               ,JSON_TABLE(P_DATA_TYPE_ARRAY,       '$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(128) PATH '$')) t
               ,JSON_TABLE(P_SIZE_CONSTRAINT_ARRAY, '$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(32) PATH '$')) s
          where (c."KEY" = t."KEY") and (c."KEY" = s."KEY")
    ),
    "TARGET_TABLE_DEFINITIONS" 
    as (
      select st.*
             -- AVOID ERROR 1271 (HY000) at line 37: Illegal mix of collations for operation 'concat'
            ,CAST(MAP_FOREIGN_DATATYPE(P_SOURCE_VENDOR,"DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE",P_CIRCLE_FORMAT) as CHAR) TARGET_DATA_TYPE
        from "SOURCE_TABLE_DEFINITIONS" st
    )
    select group_concat(concat('"',COLUMN_NAME,'"') order by "IDX" separator ',') "COLUMN_LIST"
          ,group_concat(concat('"',COLUMN_NAME,'" ',
                               case
                                 when TARGET_DATA_TYPE = 'boolean' then
                                   'tinyint(1)'
                                 when TARGET_DATA_TYPE like '%(%)' then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%unsigned'  then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('tinyint','smallint','mediumint','int','bigint','date','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum')  then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection') then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('nchar','nvarchar')  then
                                   -- then concat('(',DATA_TYPE_LENGTH,')',' CHARACTER SET UTF8MB4 ')
                                   concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),')')
                                 when DATA_TYPE_SCALE is not NULL then
                                   concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),',',CAST(DATA_TYPE_SCALE as CHAR),')')
                                 when DATA_TYPE_LENGTH is not NULL and DATA_TYPE_LENGTH != 'null' then
                                   case 
                                     when TARGET_DATA_TYPE in ('double') then
                                       -- Do not add length restriction when scale is not specified
                                       TARGET_DATA_TYPE
                                     else
                                       concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),')')
                                   end
                                 else
                                   TARGET_DATA_TYPE
                               end
                              )
                     order by "IDX" separator '  ,'
                    ) COLUMNS_CLAUSE
        ,concat('[',group_concat(JSON_QUOTE(
                               case
                                 when TARGET_DATA_TYPE = 'boolean' then
                                   'tinyint(1)'
                                 when TARGET_DATA_TYPE like '%(%)' then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%unsigned'  then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('tinyint','smallint','mediumint','int','bigint''date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum')  then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection') then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('nchar','nvarchar') then
                                   concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),')',' CHARACTER SET UTF8MB4 ')
                                 when DATA_TYPE_SCALE is not NULL  then
                                   concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),',',CAST(DATA_TYPE_SCALE AS CHAR),')')
                                 when DATA_TYPE_LENGTH is not NULL and DATA_TYPE_LENGTH <> 0 then
                                   case 
                                     when TARGET_DATA_TYPE in ('double') then
                                       -- Do not add length restriction when scale is not specified
                                       TARGET_DATA_TYPE                                        
                                     else
                                       concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),')')
                                    end
                                 else
                                   TARGET_DATA_TYPE
                               end
                              )
                     order by "IDX" separator '  ,'
                    ),']') TARGET_DATA_TYPES
          ,group_concat(concat(case
                                when TARGET_DATA_TYPE in ('timestamp','datetime') then
                                  concat('convert_tz(data."',COLUMN_NAME,'",''+00:00'',@@session.time_zone)')
                                when TARGET_DATA_TYPE IN ('varchar','text','mediumtext','longtext') or TARGET_DATA_TYPE like 'varchar(%)%' then
                                  -- Bug #93498: JSON_TABLE does not handle JSON NULL correctly with VARCHAR
                                  -- Assume the string 'null' should be the SQL NULL
                                  concat('case when data."',column_name, '" = ''null'' then NULL else data."',column_name,'" end')
                                when TARGET_DATA_TYPE = 'geometry' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_GeomFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'point' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_PointFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'linestring' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_LineStringFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'polygon' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_PolygonFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'geometrycollection' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_GeometryCollectionFromText((data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'multipoint' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_MultiPointFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'multilinestring' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_MultiLineStringFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'multipolygon' and P_SPATIAL_FORMAT in ('WKT','EWKT') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_MultiPolygonFromText(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE = 'geometry' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_GeomFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'point' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_PointFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'linestring' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_LineStringFromWKB(UNHEX(data."',COLUMN_NAME,')") end')
                                when TARGET_DATA_TYPE = 'polygon' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_PolygonFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'geometrycollection' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_GeometryCollectionFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'multipoint' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_MultiPointFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'multilinestring' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_MultiLineStringFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'multipolygon' and P_SPATIAL_FORMAT in ('WKB','EWKB') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_MultiPolygonFromWKB(UNHEX(data."',COLUMN_NAME,'")) end')
                                when TARGET_DATA_TYPE = 'geometry' and P_SPATIAL_FORMAT in ('GeoJSON') then
                                  concat('case when data."',column_name, '" = ''null'' then NULL else ST_GeomFromGeoJSON(data."',COLUMN_NAME,'") end')
                                when TARGET_DATA_TYPE like '%blob' then
                                  concat('UNHEX(data."',COLUMN_NAME,'")')
                                when TARGET_DATA_TYPE like '%binary%' then
                                  concat('UNHEX(data."',COLUMN_NAME,'")')
                                else
                                  concat('data."',COLUMN_NAME,'"')
                               end
                              ) 
                     separator ','
                    ) INSERT_SELECT_LIST
          ,group_concat(concat('"',COLUMN_NAME,'" ',
                               case
                                 when TARGET_DATA_TYPE = 'boolean' then
                                   'tinyint(1)'
                                 when TARGET_DATA_TYPE like '%(%)' then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%unsigned'  then
                                   TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%blob'  then
                                   'longtext'
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','geometrycollection','multipoint','multilinestring','multipolygon') then
                                   'varchar(4096)'
                                 when TARGET_DATA_TYPE in ('tinyint','smallint','mediumint','int','bigint','date','time','tinytext','mediumtext','text','longtext','json','set','enum') then
                                   TARGET_DATA_TYPE
                                 when DATA_TYPE_SCALE is not NULL then
                                   concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,',',DATA_TYPE_SCALE,')')
                                 when DATA_TYPE_LENGTH is not NULL and DATA_TYPE_LENGTH <> 0 then
                                   case 
                                     when TARGET_DATA_TYPE in ('double') then
                                       -- Do not add length restriction unless scale is specified
                                       TARGET_DATA_TYPE
                                     else
                                       concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,')')
                                   end
                                 else
                                   TARGET_DATA_TYPE
                               end,
                               ' PATH ''$[',IDX-1,']'' NULL ON ERROR',CHAR(32)
                              )
                     order by "IDX" separator '    ,'
                    ) COLUMN_PATTERNS 
      into V_COLUMN_LIST, V_COLUMNS_CLAUSE, V_TARGET_DATA_TYPES, V_INSERT_SELECT_LIST, V_COLUMN_PATTERNS
      from "TARGET_TABLE_DEFINITIONS";

  select  V_COLUMN_LIST, V_COLUMNS_CLAUSE, V_TARGET_DATA_TYPES, V_INSERT_SELECT_LIST, V_COLUMN_PATTERNS;

  SET V_DDL_STATEMENT = concat('create table if not exists "',P_TARGET_SCHEMA,'"."',P_TABLE_NAME,'"(',V_COLUMNS_CLAUSE,')'); 
  SET V_DML_STATEMENT = concat('insert into "',P_TARGET_SCHEMA,'"."',P_TABLE_NAME,'"(',V_COLUMN_LIST,')',CHAR(32),'select ',V_INSERT_SELECT_LIST,CHAR(32),'  from "YADAMU_STAGING" js,JSON_TABLE(js."DATA",''$.data."',P_TABLE_NAME,'"[*]'' COLUMNS (',CHAR(32),V_COLUMN_PATTERNS,')) data');     
  SET P_TABLE_INFO = JSON_OBJECT('ddl',V_DDL_STATEMENT,'dml',V_DML_STATEMENT,'targetDataTypes',CAST(V_TARGET_DATA_TYPES as JSON));
    
END;
$$
--
DELIMITER ;
--
DROP PROCEDURE IF EXISTS YADAMU_IMPORT;
--
DROP PROCEDURE IF EXISTS IMPORT_JSON;
--
DELIMITER $$
--
CREATE PROCEDURE YADAMU_IMPORT(P_TARGET_SCHEMA VARCHAR(128), OUT P_RESULTS JSON) 
BEGIN
  DECLARE NO_MORE_ROWS             INT DEFAULT FALSE;
  
  DECLARE V_VENDOR                 VARCHAR(32);
  DECLARE V_TABLE_NAME             VARCHAR(128);
  DECLARE V_SPATIAL_FORMAT         VARCHAR(7);
  DECLARE V_CIRCLE_FORMAT          VARCHAR(7);
  DECLARE V_COLUMN_NAME_ARRAY      JSON;
  DECLARE V_DATA_TYPE_ARRAY        JSON;
  DECLARE V_SIZE_CONSTRAINT_ARRAY  JSON;
  DECLARE V_TABLE_INFO             JSON;
  
  DECLARE V_STATEMENT              TEXT;
  DECLARE V_START_TIME             BIGINT;
  DECLARE V_END_TIME               BIGINT;
  DECLARE V_ELAPSED_TIME           BIGINT;
  DECLARE V_ROW_COUNT              BIGINT;
  
  DECLARE V_SQLSTATE               INT;
  DECLARE V_SQLERRM                TEXT;


  
  DECLARE TABLE_METADATA 
  CURSOR FOR 
  select VENDOR, TABLE_NAME, SPATIAL_FORMAT, CIRCLE_FORMAT, COLUMN_NAME_ARRAY, DATA_TYPE_ARRAY, SIZE_CONSTRAINT_ARRAY
    from YADAMU_STAGING js,
         JSON_TABLE(
           js.DATA,
           '$'
           COLUMNS (
             VENDOR                           VARCHAR(32) PATH '$.systemInformation.vendor',
             SPATIAL_FORMAT                  VARCHAR(128) PATH '$.systemInformation.typeMappings.spatialFormat',
             CIRCLE_FORMAT                   VARCHAR(128) PATH '$.systemInformation.typeMappings.circleFormat',
             NESTED PATH '$.metadata.*' 
               COLUMNS (
                OWNER                        VARCHAR(128) PATH '$.tableSchema'
               ,TABLE_NAME                   VARCHAR(128) PATH '$.tableName'
               ,COLUMN_NAME_ARRAY                    JSON PATH '$.columnNames'
               ,DATA_TYPE_ARRAY                      JSON PATH '$.dataTypes'
               ,SIZE_CONSTRAINT_ARRAY                JSON PATH '$.sizeConstraints'
             )
          )) c;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET NO_MORE_ROWS = TRUE;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT;
    SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('error',JSON_OBJECT('severity','FATAL','tableName', V_TABLE_NAME,'sqlStatement', V_STATEMENT, 'code', V_SQLSTATE, 'msg', V_SQLERRM, 'details', 'YADAMU_IMPORT' )));
  END;  

  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  
  SET P_RESULTS = '[]';
  SET NO_MORE_ROWS = FALSE;
  
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_VENDOR, V_TABLE_NAME, V_SPATIAL_FORMAT, V_CIRCLE_FORMAT, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
    
    IF (V_TABLE_NAME IS NULL) THEN
      LEAVE PROCESS_TABLE;
    END IF; 
    
    CALL GENERATE_SQL(V_VENDOR, P_TARGET_SCHEMA, V_TABLE_NAME, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY, V_SPATIAL_FORMAT, V_CIRCLE_FORMAT, V_TABLE_INFO);
        
    SET V_STATEMENT = JSON_UNQUOTE(JSON_EXTRACT(V_TABLE_INFO,'$.ddl'));
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
    SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('ddl',JSON_OBJECT('tableName', V_TABLE_NAME,'sqlStatement', V_STATEMENT)));
    
    SET V_STATEMENT = JSON_UNQUOTE(JSON_EXTRACT(V_TABLE_INFO,'$.dml'));
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    SET V_START_TIME = floor(unix_timestamp(current_timestamp(3)) * 1000);
    EXECUTE STATEMENT;
    SET V_ROW_COUNT = ROW_COUNT();
    SET V_END_TIME =floor(unix_timestamp(current_timestamp(3)) * 1000);
    DEALLOCATE PREPARE STATEMENT;
   
    SET V_ELAPSED_TIME = V_END_TIME - V_START_TIME;
    SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('dml',JSON_OBJECT('tableName', V_TABLE_NAME, 'rowCount', V_ROW_COUNT, 'elapsedTime',V_ELAPSED_TIME, 'sqlStatement', V_STATEMENT)));
  END LOOP;
 
  CLOSE TABLE_METADATA;
  COMMIT;
end;
$$
--
DELIMITER ;
--
DROP FUNCTION IF EXISTS GENERATE_STATEMENTS;
--
DROP PROCEDURE IF EXISTS GENERATE_STATEMENTS;
--
DELIMITER $$
--
CREATE PROCEDURE GENERATE_STATEMENTS(P_METADATA JSON, P_TARGET_SCHEMA VARCHAR(128), P_TYPE_METADATA JSON, OUT P_RESULTS JSON)
BEGIN
  DECLARE NO_MORE_ROWS             INT DEFAULT FALSE;
  
  DECLARE V_VENDOR                 VARCHAR(32);
  DECLARE V_TABLE_NAME             VARCHAR(128);
  DECLARE V_COLUMN_NAME_ARRAY      JSON;
  DECLARE V_DATA_TYPE_ARRAY        JSON;
  DECLARE V_SIZE_CONSTRAINT_ARRAY  JSON;
  DECLARE V_TABLE_INFO             JSON;
  
  DECLARE V_SPATIAL_FORMAT         VARCHAR(7);
  DECLARE V_CIRCLE_FORMAT          VARCHAR(7);
  
  
  DECLARE V_SQLSTATE               INT;
  DECLARE V_SQLERRM                TEXT;
  
  
  DECLARE TABLE_METADATA 
  CURSOR FOR 
  select VENDOR, TABLE_NAME, COLUMN_NAME_ARRAY, DATA_TYPE_ARRAY, SIZE_CONSTRAINT_ARRAY
    from JSON_TABLE(
           P_METADATA,
           '$.metadata.*' 
           COLUMNS (
                VENDOR                       VARCHAR(32)  PATH '$.vendor'
               ,OWNER                        VARCHAR(128) PATH '$.tableSchema'
               ,TABLE_NAME                   VARCHAR(128) PATH '$.tableName'
               ,COLUMN_NAME_ARRAY                    JSON PATH '$.columnNames'
               ,DATA_TYPE_ARRAY                      JSON PATH '$.dataTypes'
               ,SIZE_CONSTRAINT_ARRAY                JSON PATH '$.sizeConstraints'
             )
          ) c;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET NO_MORE_ROWS = TRUE;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT;
        SET P_RESULTS = JSON_ARRAY_APPEND(
                          P_RESULTS,
                          '$',
                          JSON_OBJECT('error',
                            JSON_OBJECT(
                              'severity',        'FATAL',
                              'tableName',       V_TABLE_NAME,
                              'columnName',      V_COLUMN_NAME_ARRAY, 
                              'dataTypes',       V_DATA_TYPE_ARRAY,
                              'sizeConstraints', V_SIZE_CONSTRAINT_ARRAY,
                              'code',            V_SQLSTATE, 
                              'msg',             V_SQLERRM, 
                              'details',         'GENERATE_STATEMENTS' 
                            )
                          )
                        );

  END;  

  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  SET P_RESULTS = '{}';
 
 
  SET V_SPATIAL_FORMAT = JSON_VALUE(P_TYPE_METADATA, '$.spatialFormat');
  SET V_CIRCLE_FORMAT = JSON_VALUE(P_TYPE_METADATA, '$.circleFormat');
 
  SET NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_VENDOR, V_TABLE_NAME, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
    
    CALL GENERATE_SQL(V_VENDOR, P_TARGET_SCHEMA, V_TABLE_NAME, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY, V_SPATIAL_FORMAT, V_CIRCLE_FORMAT, V_TABLE_INFO);
    SET P_RESULTS = JSON_INSERT(P_RESULTS,concat('$."',V_TABLE_NAME,'"'),V_TABLE_INFO);
     
  END LOOP;
 
  CLOSE TABLE_METADATA;
end;
$$
--
DELIMITER ;
--
select YADAMU_INSTANCE_ID(), YADAMU_INSTALLATION_TIMESTAMP();
--