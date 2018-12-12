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
CREATE FUNCTION MAP_FOREIGN_DATATYPE(P_SOURCE_VENDOR VARCHAR(128), P_DATA_TYPE VARCHAR(128), P_DATA_TYPE_LENGTH INT, P_DATA_TYPE_SIZE INT) 
RETURNS VARCHAR(128) DETERMINISTIC
BEGIN

  case P_SOURCE_VENDOR
    when 'Oracle'
      -- Oracle Mappings
      then case P_DATA_TYPE
             when 'VARCHAR2' 
               then return 'varchar';
             when 'NUMBER'
               then return 'decimal';
             when 'CLOB'
               then return 'longtext';
             when 'BLOB'
               then return 'longblob';
             when 'NCLOB'
               then return 'longtext';
             when 'BFILE'
               then return 'varchar(2048)';
             when 'ROWID'
               then return 'varchar(32)';
             when 'XMLTYPE'
               then return 'longtext';
             when 'RAW'
               then return 'binary';
             when 'NVARCHAR2'
               then return 'varchar';
             when 'ANYDATA'
               then return 'longtext';
             else
               -- Oracle Special Cases
               if (instr(P_DATA_TYPE,'TIME ZONE') > 0) then
                 return 'timestamp'; 
               end if;
               if ((instr(P_DATA_TYPE,'INTERVAL') = 1)) then
                 return 'varchar(16)';
               end if;
               if (INSTR(P_DATA_TYPE,'"."') > 0) then 
                 return 'text';
               end if;
               return lower(P_DATA_TYPE);
           end case;
    when 'MSSQLSERVER'
      then case P_DATA_TYPE
             -- SQLServer Mapppings
             when 'binary'
               then case 
                      when P_DATA_TYPE_LENGTH > 16777215 then return 'longblob';
                      when P_DATA_TYPE_LENGTH > 65535  then return 'mediumblob';
                      when P_DATA_TYPE_LENGTH > 255  then return 'blob';
                      else return 'tinyblob';
                    end case;
             when 'bit'
               then return 'tinyint(1)';
             when 'char'
               then case 
                      when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 16777215 then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
                      when P_DATA_TYPE_LENGTH > 255  then return 'text';
                      else return 'char';
                    end case;
             when 'datetime'
               then return 'datetime(3)';
             when 'datetime2'
               then return 'datatime';
             when 'datetimeoffset'
               then return 'datatime';
             when 'geography'
             -- ###TODO : Solve mapping MSSQL geography to MYSQL
               then return 'json';
             when 'geometry'
             -- ###TODO : Solve mapping MSSQL geometry to MYSQL
               then return 'json';
             when 'hierarchyid'
               then return 'varchar(4000)';
             when 'image'
               then return 'longblob';
             when 'mediumint'
               then return 'int';
             when 'money'
               then return 'decimal(19,4)';
             when 'nchar'
               then case 
                      when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 16777215  then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
                      when P_DATA_TYPE_LENGTH > 255  then return 'text';
                      else return 'char';
                    end case;
             when 'ntext'
               then return 'longtext';
             when 'nvarchar'
               then case
                      when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 16777215  then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
                      when P_DATA_TYPE_LENGTH > 255  then return 'text';
                      else return 'varchar';
                    end case;
             when 'real'
               then return 'float';
             when 'rowversion'
               then return 'binary(8)';
             when 'smalldate'
               then return 'datatime';
             when 'smallmoney'
               then return 'decimal(10,4)';
             when 'text'
               then return 'longtext';
             when 'tinyint'
               then return 'smallint';
             when 'uniqueidentifier'
               then return 'varchar(64)';
             when 'varbinary'
               then case
                      when P_DATA_TYPE_LENGTH = -1 then return 'longblob';
                      when P_DATA_TYPE_LENGTH > 16777215  then return 'longblob';
                      when P_DATA_TYPE_LENGTH > 65535  then return 'mediumblob';
                      else return 'varbinary';
                    end case;
             when 'varchar'
               then case
                      when P_DATA_TYPE_LENGTH = -1 then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 16777215  then return 'longtext';
                      when P_DATA_TYPE_LENGTH > 65535  then return 'mediumtext';
                      else return 'varchar';
                    end case;
             when 'xml'
               then return 'longtext';
             else
               return lower(P_DATA_TYPE);
           end case;
    when 'Postges'
      then return lower(P_DATA_TYPE);
    when 'MySQL'
      then case P_DATA_TYPE
             -- Metadata does not contain sufficinet infromation to rebuild ENUM and SET data types. Enable roundtrip by mappong ENUM and SET to TEXT.
             when 'set' 
               then return 'varchar(512)';
             when 'enum' 
               then return 'varchar(512)';
             else
               return lower(P_DATA_TYPE);
           end case;       
    when 'MariaDB'
      then case P_DATA_TYPE
             -- Metadata does not contain sufficnet infromation to rebuild ENUM and SET data types. Enable roundtrip by mappong ENUM and SET to TEXT.
             when 'set' 
               then return 'varchar(512)';
             when 'enum' 
               then return 'varchar(512)';
             else
               return lower(P_DATA_TYPE);
           end case;       
    else
      return lower(P_DATA_TYPE);
  end case;
end;
$$
--
DELIMITER ;
--
DELIMITER $$
--
DROP PROCEDURE IF EXISTS GENERATE_SQL;
--
CREATE PROCEDURE GENERATE_SQL(P_SOURCE_VENDOR VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128), P_TABLE_NAME VARCHAR(128), P_COLUMN_LIST TEXT, P_DATA_TYPE_LIST JSON, P_SIZE_CONSTRAINTS JSON, OUT P_TABLE_INFO JSON)
BEGIN
  DECLARE V_COLUMNS_CLAUSE     TEXT;
  DECLARE V_INSERT_SELECT_LIST TEXT;
  DECLARE V_COLUMN_PATTERNS    TEXT;
  DECLARE V_DML_STATEMENT      TEXT;
  DECLARE V_DDL_STATEMENT      TEXT;
  DECLARE V_TARGET_DATA_TYPES  TEXT;
  
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
           from JSON_TABLE(CONCAT('[',P_COLUMN_LIST,']'),'$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(128) PATH '$')) c
               ,JSON_TABLE(P_DATA_TYPE_LIST,'$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(128) PATH '$')) t
               ,JSON_TABLE(P_SIZE_CONSTRAINTS,'$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(32) PATH '$')) s
          where (c."KEY" = t."KEY") and (c."KEY" = s."KEY")
    ),
    "TARGET_TABLE_DEFINITIONS" 
    as (
      select st.*
            , MAP_FOREIGN_DATATYPE(P_SOURCE_VENDOR,"DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
        from "SOURCE_TABLE_DEFINITIONS" st
    )
    select group_concat(concat('"',COLUMN_NAME,'" ',TARGET_DATA_TYPE,
                               case
                                 when TARGET_DATA_TYPE like '%(%)'
                                   then ''
                                 when TARGET_DATA_TYPE like '%unsigned' 
                                   then ''
                                 when TARGET_DATA_TYPE in ('date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum') 
                                   then ''
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')
                                   then '' 
                                 when DATA_TYPE in ('nchar','nvarchar')
                                   then concat('(',DATA_TYPE_LENGTH,')',' CHARACTER SET UTF8MB4 ')
                                 when DATA_TYPE_SCALE is not NULL
                                   then case 
                                          when DATA_TYPE in ('tinyint','smallint','mediumint','int','bigint') 
                                            then concat('(',DATA_TYPE_LENGTH,')')
                                          else
                                            concat('(',DATA_TYPE_LENGTH,',',DATA_TYPE_SCALE,')')
                                        end
                                 when DATA_TYPE_LENGTH is not NULL
                                   then case 
                                          when TARGET_DATA_TYPE in ('double')
                                            -- Do not add length restriction when scale is not specified
                                            then ''                                            
                                          else
                                            concat('(',DATA_TYPE_LENGTH,')')
                                        end
                                 else
                                   ''
                               end,
                               CHAR(32)
                              )
                     order by "IDX" separator '  ,'
                    ) COLUMNS_CLAUSE
        ,concat('[',group_concat(JSON_QUOTE(
                               case
                                 when TARGET_DATA_TYPE like '%(%)'
                                   then TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%unsigned' 
                                   then TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum') 
                                   then TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')
                                   then TARGET_DATA_TYPE
                                 when DATA_TYPE in ('nchar','nvarchar')
                                   then concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,')',' CHARACTER SET UTF8MB4 ')
                                 when DATA_TYPE_SCALE is not NULL
                                   then case 
                                          when DATA_TYPE in ('tinyint','smallint','mediumint','int','bigint') 
                                            then concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,')')
                                          else
                                            concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,',',DATA_TYPE_SCALE,')')
                                        end
                                 when DATA_TYPE_LENGTH is not NULL and DATA_TYPE_LENGTH <> 0
                                   then case 
                                          when TARGET_DATA_TYPE in ('double')
                                            -- Do not add length restriction when scale is not specified
                                            then TARGET_DATA_TYPE                                        
                                          else
                                            concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,')')
                                        end
                                 else
                                   TARGET_DATA_TYPE
                               end
                              )
                     order by "IDX" separator '  ,'
                    ),']') TARGET_DATA_TYPES
          ,group_concat(concat(case
                                when TARGET_DATA_TYPE = 'timestamp' 
                                  then concat('convert_tz(data."',COLUMN_NAME,'",''+00:00'',@@session.time_zone)')
                                when TARGET_DATA_TYPE IN ('varchar','text')
                                  -- Bug #93498: JSON_TABLE does not handle JSON NULL correctly with VARCHAR
                                  -- Assume the string 'null' should be the SQL NULL
                                   then concat('case when data."',column_name, '" = ''null'' then NULL else data."',column_name,'" end')
                                when TARGET_DATA_TYPE = 'geometry' 
                                  then concat('ST_GEOMFROMGEOJSON(data."',COLUMN_NAME,'")')
                                when TARGET_DATA_TYPE like '%blob'
                                  then concat('UNHEX(data."',COLUMN_NAME,'")')
                                when TARGET_DATA_TYPE like '%binary%'
                                  then concat('UNHEX(data."',COLUMN_NAME,'")')
                                else
                                  concat('data."',COLUMN_NAME,'"')
                               end
                              ) 
                     separator ','
                    ) INSERT_SELECT_LIST
          ,group_concat(concat('"',COLUMN_NAME,'" ',
                               case
                                 when TARGET_DATA_TYPE like '%(%)'
                                   then TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%unsigned' 
                                   then TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE like '%blob' 
                                   then 'longtext'
                                 when TARGET_DATA_TYPE in ('geometry','geography')
                                   then 'json'
                                 when TARGET_DATA_TYPE in ('date','time','tinytext','mediumtext','text','longtext','json','set','enum') 
                                   then TARGET_DATA_TYPE
                                 when TARGET_DATA_TYPE in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')
                                   then TARGET_DATA_TYPE 
                                 when DATA_TYPE_SCALE is not NULL
                                   then case 
                                          when DATA_TYPE in ('tinyint','smallint','mediumint','int','bigint') 
                                            then concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,')')
                                          else
                                            concat(TARGET_DATA_TYPE,'(',DATA_TYPE_LENGTH,',',DATA_TYPE_SCALE,')')
                                        end
                                 when DATA_TYPE_LENGTH is not NULL and DATA_TYPE_LENGTH <> 0
                                   then case 
                                          when TARGET_DATA_TYPE in ('double')
                                            -- Do not add length restriction when scale is not specified
                                            then TARGET_DATA_TYPE
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
      into V_COLUMNS_CLAUSE, V_TARGET_DATA_TYPES, V_INSERT_SELECT_LIST, V_COLUMN_PATTERNS
      from "TARGET_TABLE_DEFINITIONS";
      
     
    SET V_DDL_STATEMENT = concat('create table if not exists "',P_TARGET_SCHEMA,'"."',P_TABLE_NAME,'"(',CHAR(32),V_COLUMNS_CLAUSE,')'); 
    SET V_DML_STATEMENT = concat('insert into "',P_TARGET_SCHEMA,'"."',P_TABLE_NAME,'"(',P_COLUMN_LIST,')',CHAR(32),'select ',V_INSERT_SELECT_LIST,CHAR(32),'  from "JSON_STAGING" js,JSON_TABLE(js."DATA",''$.data."',P_TABLE_NAME,'"[*]'' COLUMNS (',CHAR(32),V_COLUMN_PATTERNS,')) data');
    
    SET P_TABLE_INFO = JSON_OBJECT('ddl',V_DDL_STATEMENT,'dml',V_DML_STATEMENT,'targetDataTypes',CAST(V_TARGET_DATA_TYPES as JSON));
    -- SET P_TABLE_INFO = JSON_OBJECT('ddl',V_DDL_STATEMENT,'dml',V_DML_STATEMENT,'targetDataTypes',V_TARGET_DATA_TYPES);
    
END;
$$
--
DELIMITER ;
--
DROP PROCEDURE IF EXISTS IMPORT_JSON;
--
DELIMITER $$
--
CREATE PROCEDURE IMPORT_JSON(P_TARGET_SCHEMA VARCHAR(128), OUT P_RESULTS JSON) 
BEGIN
  DECLARE NO_MORE_ROWS        INT DEFAULT FALSE;
  
  DECLARE V_VENDOR           VARCHAR(32);
  DECLARE V_TABLE_NAME       VARCHAR(128);
DECLARE V_COLUMN_LIST      TEXT;
  DECLARE V_DATA_TYPE_LIST   JSON;
  DECLARE V_SIZE_CONSTRAINTS JSON;
  DECLARE V_TABLE_INFO       JSON;
  
  DECLARE V_STATEMENT         TEXT;
  DECLARE V_START_TIME        BIGINT;
  DECLARE V_END_TIME          BIGINT;
  DECLARE V_ELAPSED_TIME      BIGINT;
  DECLARE V_ROW_COUNT         BIGINT;
  
  DECLARE V_SQLSTATE          INT;
  DECLARE V_SQLERRM           TEXT;
  
  DECLARE TABLE_METADATA 
  CURSOR FOR 
  select VENDOR, TABLE_NAME, COLUMN_LIST, DATA_TYPE_LIST, SIZE_CONSTRAINTS
    from JSON_STAGING js,
         JSON_TABLE(
           js.DATA,
           '$'
           COLUMNS (
             VENDOR                           VARCHAR(32) PATH '$.systemInformation.vendor',
             NESTED PATH '$.metadata.*' 
               COLUMNS (
                OWNER                        VARCHAR(128) PATH '$.owner'
               ,TABLE_NAME                   VARCHAR(128) PATH '$.tableName'
               ,COLUMN_LIST                          TEXT PATH '$.columns'
               ,DATA_TYPE_LIST                       JSON PATH '$.dataTypes'
               ,SIZE_CONSTRAINTS                     JSON PATH '$.sizeConstraints'
               ,INSERT_SELECT_LIST                   TEXT PATH '$.insertSelectList'
               ,COLUMN_PATTERNS                      TEXT PATH '$.columnPatterns'
             )
          )) c;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET NO_MORE_ROWS = TRUE;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT;
    SET P_RESULTS = JSON_ARRAY_APPEND(P_RESULTS,'$',JSON_OBJECT('error',JSON_OBJECT('severity','FATAL','tableName', V_TABLE_NAME,'sqlStatement', V_STATEMENT, 'code', V_SQLSTATE, 'msg', V_SQLERRM, 'details', 'unavailable' )));
  END;  

  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  
  SET P_RESULTS = '[]';
  SET NO_MORE_ROWS = FALSE;
  
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_VENDOR, V_TABLE_NAME, V_COLUMN_LIST, V_DATA_TYPE_LIST, V_SIZE_CONSTRAINTS;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
    
    IF (V_TABLE_NAME IS NULL) THEN
      LEAVE PROCESS_TABLE;
    END IF; 
    
    CALL GENERATE_SQL(V_VENDOR, P_TARGET_SCHEMA, V_TABLE_NAME, V_COLUMN_LIST, V_DATA_TYPE_LIST, V_SIZE_CONSTRAINTS, V_TABLE_INFO);
      
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
CREATE PROCEDURE GENERATE_STATEMENTS(P_METADATA JSON, P_TARGET_SCHEMA VARCHAR(128), OUT P_RESULTS JSON)
BEGIN
  DECLARE NO_MORE_ROWS       INT DEFAULT FALSE;
  
  DECLARE V_VENDOR           VARCHAR(32);
  DECLARE V_TABLE_NAME       VARCHAR(128);
  DECLARE V_COLUMN_LIST      TEXT;
  DECLARE V_DATA_TYPE_LIST   JSON;
  DECLARE V_SIZE_CONSTRAINTS JSON;
  DECLARE V_TABLE_INFO       JSON;
  
  DECLARE V_SQLSTATE         INT;
  DECLARE V_SQLERRM          TEXT;
  
  DECLARE TABLE_METADATA 
  CURSOR FOR 
  select VENDOR, TABLE_NAME, COLUMN_LIST, DATA_TYPE_LIST, SIZE_CONSTRAINTS
    from JSON_TABLE(
           P_METADATA,
           '$'
           COLUMNS (
             VENDOR                           VARCHAR(32) PATH '$.systemInformation.vendor',
             NESTED                                       PATH '$.metadata.*' 
               COLUMNS (
                OWNER                        VARCHAR(128) PATH '$.owner'
               ,TABLE_NAME                   VARCHAR(128) PATH '$.tableName'
               ,COLUMN_LIST                          TEXT PATH '$.columns'
               ,DATA_TYPE_LIST                       JSON PATH '$.dataTypes'
               ,SIZE_CONSTRAINTS                     JSON PATH '$.sizeConstraints'
               ,INSERT_SELECT_LIST                   TEXT PATH '$.insertSelectList'
               ,COLUMN_PATTERNS                      TEXT PATH '$.columnPatterns'
             )
          )) c;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET NO_MORE_ROWS = TRUE;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT;
    SET P_RESULTS = JSON_OBJECT('error',JSON_OBJECT('severity','FATAL','tableName', V_TABLE_NAME,'code', V_SQLSTATE, 'msg', V_SQLERRM, 'results', P_RESULTS ));
  END;  

  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  SET P_RESULTS = '{}';
 
  SET NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_VENDOR, V_TABLE_NAME, V_COLUMN_LIST, V_DATA_TYPE_LIST, V_SIZE_CONSTRAINTS;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
    
    CALL GENERATE_SQL(V_VENDOR, P_TARGET_SCHEMA, V_TABLE_NAME, V_COLUMN_LIST, V_DATA_TYPE_LIST, V_SIZE_CONSTRAINTS, V_TABLE_INFO);
    SET P_RESULTS = JSON_INSERT(P_RESULTS,concat('$."',V_TABLE_NAME,'"'),V_TABLE_INFO);
     
  END LOOP;
 
  CLOSE TABLE_METADATA;
end;
$$
--
DELIMITER ;
--
DROP PROCEDURE IF EXISTS COMPARE_SCHEMAS;
--
DELIMITER $$
--
CREATE PROCEDURE COMPARE_SCHEMAS(P_SOURCE_SCHEMA VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128))
BEGIN
  DECLARE C_NEWLINE          VARCHAR(1) DEFAULT CHAR(13);
  
  DECLARE C_SCHEMA_COMPARE_RESULTS VARCHAR(4000) DEFAULT 
'create temporary table if not exists SCHEMA_COMPARE_RESULTS (
  SOURCE_SCHEMA    VARCHAR(128)
 ,TARGET_SCHEMA    VARCHAR(128)
 ,TABLE_NAME       VARCHAR(128)
 ,SOURCE_ROW_COUNT INT
 ,TARGET_ROW_COUNT INT
 ,MISSINGS_ROWS    INT
 ,EXTRA_ROWS       INT
)';

  DECLARE NO_MORE_ROWS       INT DEFAULT FALSE;
  DECLARE V_TABLE_NAME       VARCHAR(128);
  DECLARE V_COLUMN_LIST      TEXT;
  DECLARE V_STATEMENT        TEXT;
  
  DECLARE V_SQLSTATE         INT;
  DECLARE V_SQLERRM          TEXT;
  
  DECLARE TABLE_METADATA 
  CURSOR FOR 
  select c.table_name "TABLE_NAME"
        ,group_concat(concat('"',column_name,'"') order by ordinal_position separator ',')  "COLUMNS"
   from (
     select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,column_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision
       from information_schema.columns c, information_schema.tables t
       where t.table_name = c.table_name 
         and c.extra <> 'VIRTUAL GENERATED'
         and t.table_schema = c.table_schema
         and t.table_type = 'BASE TABLE'
         and t.table_schema = P_SOURCE_SCHEMA
   ) c
  group by c.table_schema, c.table_name;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET NO_MORE_ROWS = TRUE;
  
  /*
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN 
    GET DIAGNOSTICS CONDITION 1
        V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT;
    SET P_RESULTS = JSON_OBJECT('error',JSON_OBJECT('severity','FATAL','tableName', V_TABLE_NAME,'code', V_SQLSTATE, 'msg', V_SQLERRM, 'results', P_RESULTS ));
  END;  
  */
  
  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  
  SET @STATEMENT = C_SCHEMA_COMPARE_RESULTS;
  PREPARE STATEMENT FROM @STATEMENT;
  EXECUTE STATEMENT;
  DEALLOCATE PREPARE STATEMENT;

  DELETE FROM SCHEMA_COMPARE_RESULTS;
  COMMIT;
  
  SET NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_TABLE_NAME, V_COLUMN_LIST;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
    
    SET V_STATEMENT = CONCAT('insert into SCHEMA_COMPARE_RESULTS ',C_NEWLINE,
                             ' select ''',P_SOURCE_SCHEMA,''' ',C_NEWLINE,
                             '       ,''',P_TARGET_SCHEMA,''' ',C_NEWLINE,
                             '       ,''',V_TABLE_NAME,''' ',C_NEWLINE,
                             '       ,(select count(*) from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'")',C_NEWLINE,
                             '       ,(select count(*) from "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'")',C_NEWLINE,
                             '       ,(select count(*) from (SELECT MD5(JSON_ARRAY(',V_COLUMN_LIST,')) HASH FROM "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'") T1 LEFT JOIN  (SELECT MD5(JSON_ARRAY(',V_COLUMN_LIST,')) HASH FROM "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'") T2 USING (HASH) WHERE T2.HASH IS NULL)',C_NEWLINE,
                             '       ,(select count(*) from (SELECT MD5(JSON_ARRAY(',V_COLUMN_LIST,')) HASH FROM "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'") T1 LEFT JOIN  (SELECT MD5(JSON_ARRAY(',V_COLUMN_LIST,')) HASH FROM "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'") T2 USING (HASH) WHERE T2.HASH IS NULL)');   
    
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
     
  END LOOP;
 
  CLOSE TABLE_METADATA;
end;
$$
--
DELIMITER ;





--