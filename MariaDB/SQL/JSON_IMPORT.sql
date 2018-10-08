/*
**
** MySQL JSON_IMPORT Function.
**
*/
SET SESSION SQL_MODE=ANSI_QUOTES;
--
DROP FUNCTION IF EXISTS MAP_FOREIGN_DATATYPE;
--
DELIMITER $$
--
CREATE FUNCTION MAP_FOREIGN_DATATYPE(P_DATA_TYPE VARCHAR(128), P_DATA_TYPE_LENGTH INT, P_DATA_TYPE_SIZE INT) 
RETURNS VARCHAR(128) DETERMINISTIC
BEGIN

  case P_DATA_TYPE
    -- TODO : Enable Roundtrip for ENUM and SET
	when 'set' 
	  then return 'text';
	when 'enum' 
	  then return 'text';
    -- Oracle Mappings
	when 'VARCHAR2' 
	  then return 'varchar';
	when 'NUMBER'
      then return 'decimal';
	when 'CLOB'
      then return 'text';
	when 'NCLOB'
      then return 'text';
	when 'BFILE'
      then return 'varchar(2048)';
	when 'ROWID'
      then return 'varchar(32)';
    -- SQLServer Mapppings
	when 'nchar'
      then return 'char';
	when 'tinyint'
      then return 'tinyint unsigned';
	when 'bit'
      then return 'tinyint(1)';
	when 'real'
      then return 'float';
	when 'numeric'
      then return 'decimal';
	when 'money'
      then return 'decimal';
	when 'smallmoney'
      then return 'decimal';
	when 'char'
      then case 
             when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 16777215 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
             when P_DATA_TYPE_LENGTH > 255  then return 'text';
             else return 'char';
           end case;
	when 'nchar'
      then case 
             when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 16777215  then return 'longtext';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
             when P_DATA_TYPE_LENGTH > 255  then return 'text';
             else return 'char';
           end case;
	when 'nvarchar'
      then case
             when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 16777215  then return 'longtext';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
             else return 'varchar';
           end case;
	when 'varchar'
      then case
             when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 16777215  then return 'longtext';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
             else return 'varchar';
           end case;
	when 'datetime2'
      then return 'datatime';
	when 'smalldate'
      then return 'datatime';
	when 'datetimeoffset'
      then return 'datatime';
	when 'rowversion'
      then return 'datatime';
	when 'binary'
      then case 
             when P_DATA_TYPE_LENGTH > 16777215 then return 'longblob';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumblob';
             when P_DATA_TYPE_LENGTH > 255  then return 'blob';
             else return 'tinyblob';
           end case;
	when 'varbinary'
      then case
             when P_DATA_TYPE_LENGTH = -1 then return 'longblob';
             when P_DATA_TYPE_LENGTH > 16777215  then return 'longblob';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumblob';
             else return 'varbinary';
           end case;
	when 'text'
      then case 
             when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 16777215 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
             when P_DATA_TYPE_LENGTH > 255  then return 'text';
             else return 'char';
           end case;
	when 'ntext'
      then case 
             when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 16777215 then return 'longtext';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
             when P_DATA_TYPE_LENGTH > 255  then return 'text';
             else return 'char';
           end case;
	when 'image'
      then case 
             when P_DATA_TYPE_LENGTH = -1 then return 'longblob';
             when P_DATA_TYPE_LENGTH > 16777215 then return 'longblob';
             when P_DATA_TYPE_LENGTH > 65535  then return 'mediumblob';
             when P_DATA_TYPE_LENGTH > 255  then return 'blob';
             else return 'tinyblob';
           end case;
	when 'uniqueidentifier'
      then return 'varchar(64)';
	when 'hierarchyid'
      then return 'varbinary(446)';
	when 'xml'
      then return 'longtext';
	when 'geography'
      then return 'json';
	else
   	  if (instr(@P_DATA_TYPE,'TIME ZONE') > 0) then
	    return 'timestamp';	
      end if;
	  if (INSTR(P_DATA_TYPE,'"."') > 0) then 
	    return 'text';
	  end if;
      
   	  if ((instr(P_DATA_TYPE,'INTERVAL') = 1)) then
	    return 'varchar(16)';
      end if;
	  return lower(P_DATA_TYPE);
  end case;
end;
$$
--
DELIMITER ;
--
DROP PROCEDURE IF EXISTS IMPORT_JSON;
--
DELIMITER $$
--
CREATE PROCEDURE IMPORT_JSON(P_TARGET_DATABASE VARCHAR(128), OUT P_RESULTS JSON) 
BEGIN
  DECLARE NO_MORE_ROWS      INT DEFAULT FALSE;
  
  DECLARE V_OWNER           VARCHAR(128);
  DECLARE V_TABLE_NAME      VARCHAR(128);
  DECLARE V_COLUMN_LIST     TEXT;
  DECLARE V_DATA_TYPE_LIST  TEXT;
  DECLARE V_SIZE_CONTRAINTS TEXT;

  DECLARE V_STATEMENT     TEXT;
  DECLARE V_DDL_STATEMENT TEXT;
  DECLARE V_DML_STATEMENT TEXT;
  
  DECLARE V_START_TIME    BIGINT;
  DECLARE V_END_TIME      BIGINT;
  DECLARE V_ELAPSED_TIME  BIGINT;
  
  DECLARE V_ROW_COUNT     BIGINT;
  
  DECLARE V_SQLSTATE      INT;
  DECLARE V_SQLERRM       TEXT;
  
  DECLARE V_COLUMNS_CLAUSE     TEXT;
  DECLARE V_INSERT_SELECT_LIST TEXT;
  DECLARE V_COLUMN_PATTERNS    TEXT;
  
  DECLARE TABLE_METADATA 
  CURSOR FOR 
  select OWNER
        ,TABLE_NAME
        ,COLUMN_LIST
        ,DATA_TYPE_LIST
        ,SIZE_CONSTRAINTS
    from "JSON_STAGING",
	     JSON_TABLE(
	       "DATA",
			'$.metadata.*' 
		 	COLUMNS (
			  OWNER                        VARCHAR(128) PATH '$.owner'
			 ,TABLE_NAME                   VARCHAR(128) PATH '$.tableName'
			 ,COLUMN_LIST                          TEXT PATH '$.columns'
			 ,DATA_TYPE_LIST                       TEXT PATH '$.dataTypes'
			 ,SIZE_CONSTRAINTS                     TEXT PATH '$.dataTypeSizing'
			 ,INSERT_SELECT_LIST                   TEXT PATH '$.insertSelectList'
             ,COLUMN_PATTERNS                      TEXT PATH '$.columnPatterns'
		  )) c;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET NO_MORE_ROWS = TRUE;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT;
	SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('error',JSON_OBJECT('severity','FATAL','tableName', V_TABLE_NAME,'sqlStatement', @V_STATEMENT, 'code', V_SQLSTATE, 'msg', V_SQLERRM, 'details', 'unavailable' )));
  END;  

  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  
  SET P_RESULTS = '[]';
  SET NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
  	
  PROCESS_TABLE : LOOP
	FETCH TABLE_METADATA INTO V_OWNER, V_TABLE_NAME, V_COLUMN_LIST, V_DATA_TYPE_LIST, V_SIZE_CONTRAINTS;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
   
    with 
    "SOURCE_TABLE_DEFINITIONS" 
    as ( 
      select c."KEY" IDX
            ,c.VALUE "COLUMN_NAME"
            ,t.VALUE "DATA_TYPE"
            ,case
               when s.VALUE = ''
                 then NULL
               when INSTR(s.VALUE,',') > 0
                 then SUBSTR(s.VALUE,1,INSTR(s.VALUE,',')-1)
               else
                 s.VALUE
             end "DATA_TYPE_LENGTH"
            ,case
               when INSTR(s.VALUE,',') > 0
                 then SUBSTR(s.VALUE, INSTR(s.VALUE,',')+1)
               else
                 NULL
             end "DATA_TYPE_SCALE"
           from JSON_TABLE(CONCAT('[',V_COLUMN_LIST,']'),'$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(128) PATH '$')) c
               ,JSON_TABLE(CONCAT('[',REPLACE(V_DATA_TYPE_LIST,'"."','\".\"'),']'),'$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(32) PATH '$')) t
               ,JSON_TABLE(CONCAT('[',V_SIZE_CONTRAINTS,']'),'$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(32) PATH '$')) s
          where (c."KEY" = t."KEY") and (c."KEY" = s."KEY")
    ),
    "TARGET_TABLE_DEFINITIONS" 
    as (
      select std.*
            , MAP_FOREIGN_DATATYPE("DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
        from "SOURCE_TABLE_DEFINITIONS" std
    )
    select @V_COLUMS_CLAUSE :=
           group_concat(concat('"',COLUMN_NAME,'" ',TARGET_DATA_TYPE,
                               case
                                 when TARGET_DATA_TYPE like '%(%)'
                                   then ''
                                 when TARGET_DATA_TYPE like '%unsigned' 
                                   then ''
                                 when TARGET_DATA_TYPE in ('tinyint','smallint','mediumint','int','set','enum','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json') 
                                   then ''
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')
                                   then '' 
                                 when DATA_TYPE in ('nchar','nvarchar')
                                   then concat('(',DATA_TYPE_LENGTH,')',' CHARACTER SET UTF8MB4 ')
                                 when DATA_TYPE_SCALE is not NULL
                                   then concat('(',DATA_TYPE_LENGTH,',',DATA_TYPE_SCALE,')')
                                 when DATA_TYPE_LENGTH is not NULL
                                   then case when TARGET_DATA_TYPE in ('double')
                                               then concat('(',DATA_TYPE_LENGTH,',0)')
                                             else
                                               concat('(',DATA_TYPE_LENGTH,')')
                                             end
                                 else
                                   ''
                               end,
                               CHAR(32)
                              )
                     separator '  ,'
                    ) COLUMNS_CLAUSE
          ,@V_INSERT_SELECT_LIST :=
           group_concat(concat('"',COLUMN_NAME,'"') separator ',') INSERT_SELECT_LIST
          ,@V_COLUMN_PATTERNS := 
           group_concat(concat('"',COLUMN_NAME,'" ',
                               TARGET_DATA_TYPE,
                               case
                                 when TARGET_DATA_TYPE like '%(%)'
                                   then ''
                                 when TARGET_DATA_TYPE like '%unsigned' 
                                   then ''
                                 when TARGET_DATA_TYPE in ('tinyint','smallint','mediumint','int','set','enum','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json') 
                                   then ''
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')
                                   then '' 
                                 when DATA_TYPE_SCALE is not NULL
                                   then concat('(',DATA_TYPE_LENGTH,',',DATA_TYPE_SCALE,')')
                                 when DATA_TYPE_LENGTH is not NULL
                                   then case when TARGET_DATA_TYPE in ('double')
                                               then concat('(',DATA_TYPE_LENGTH,',0)')
                                             else
                                               concat('(',DATA_TYPE_LENGTH,')')
                                        end
                                 else
                                   ''
                               end,
                               ' PATH ''$[',IDX-1,']''',CHAR(32)
                              )
                     separator '    ,'
                    ) COLUMN_PATTERNS
      from "TARGET_TABLE_DEFINITIONS";
   
    SET @V_DDL_STATEMENT := concat('create table if not exists "',P_TARGET_DATABASE,'"."',V_TABLE_NAME,'"(',CHAR(32),@V_COLUMS_CLAUSE,')'); 
    SET @V_DML_STATEMENT := concat('insert into "',P_TARGET_DATABASE,'"."',V_TABLE_NAME,'"(',V_COLUMN_LIST,')',CHAR(32),'select ',@V_INSERT_SELECT_LIST,CHAR(32),'  from "JSON_STAGING",JSON_TABLE("DATA",''$.data."',V_TABLE_NAME,'"[*]'' COLUMNS (',CHAR(32),@V_COLUMN_PATTERNS,')) data');
      
   	-- SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('info',JSON_OBJECT('DDL',@V_DDL_STATEMENT)));
   	-- SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('info',JSON_OBJECT('DML',@V_DML_STATEMENT)));
    
    SET @V_STATEMENT = @V_DDL_STATEMENT;
    PREPARE STATEMENT FROM @V_DDL_STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
	SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('ddl',JSON_OBJECT('tableName', V_TABLE_NAME,'sqlStatement', @V_DDL_STATEMENT)));
    
    SET @V_STATEMENT = @V_DML_STATEMENT;
	PREPARE STATEMENT FROM @V_DML_STATEMENT;
	SET @V_START_TIME = floor(unix_timestamp(current_timestamp(3)) * 1000);
    EXECUTE STATEMENT;
	SET @V_ROW_COUNT = ROW_COUNT();
	SET @V_END_TIME =floor(unix_timestamp(current_timestamp(3)) * 1000);
    DEALLOCATE PREPARE STATEMENT;
   
	SET @V_ELAPSED_TIME = @V_END_TIME - @V_START_TIME;
	SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('dml',JSON_OBJECT('tableName', V_TABLE_NAME, 'rowCount', @V_ROW_COUNT, 'elapsedTime',@V_ELAPSED_TIME, 'sqlStatement', @V_DML_STATEMENT)));
  END LOOP;
 
  CLOSE TABLE_METADATA;
end;
$$
--
DELIMITER ;

--
