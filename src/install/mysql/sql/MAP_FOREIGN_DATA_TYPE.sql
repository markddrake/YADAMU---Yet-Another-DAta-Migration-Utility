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
