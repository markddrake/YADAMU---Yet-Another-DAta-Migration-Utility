DROP PROCEDURE IF EXISTS COMPARE_SCHEMAS;
--
DELIMITER $$
--
CREATE PROCEDURE COMPARE_SCHEMAS(P_SOURCE_SCHEMA VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128))
BEGIN
  DECLARE C_NEWLINE          VARCHAR(1) DEFAULT CHAR(13);
  

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
  SET max_heap_table_size = 1 * 1024 *1024 *1024;
  create temporary table if not exists SCHEMA_COMPARE_RESULTS (
    SOURCE_SCHEMA    VARCHAR(128)
   ,TARGET_SCHEMA    VARCHAR(128)
   ,TABLE_NAME       VARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSINGS_ROWS    INT
   ,EXTRA_ROWS       INT
  );
  
  create temporary table if not exists SOURCE_HASH_TABLE (
    HASH    CHAR(64) PRIMARY KEY,
    CNT     INT   
  ) ENGINE=MEMORY;

  create temporary table if not exists TARGET_HASH_TABLE (
    HASH    CHAR(64) PRIMARY KEY,
    CNT     INT   
  ) ENGINE=MEMORY;
  
  TRUNCATE TABLE SCHEMA_COMPARE_RESULTS;
  COMMIT;
  
  SET NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_TABLE_NAME, V_COLUMN_LIST;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
    
    TRUNCATE TABLE SOURCE_HASH_TABLE;
    TRUNCATE TABLE TARGET_HASH_TABLE;
    
    SET V_STATEMENT = CONCAT('insert into SOURCE_HASH_TABLE select SHA2(JSON_ARRAY(',V_COLUMN_LIST,'),256) HASH, COUNT(*) CNT from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'" group by HASH');
    
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
    
    SET V_STATEMENT = CONCAT('insert into TARGET_HASH_TABLE select SHA2(JSON_ARRAY(',V_COLUMN_LIST,'),256) HASH, COUNT(*) CNT from "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'" group by HASH');
    
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;

    SET V_STATEMENT = CONCAT('insert into SCHEMA_COMPARE_RESULTS ',C_NEWLINE,
                             ' select ''',P_SOURCE_SCHEMA,''' ',C_NEWLINE,
                             '       ,''',P_TARGET_SCHEMA,''' ',C_NEWLINE,
                             '       ,''',V_TABLE_NAME,''' ',C_NEWLINE,
                             '       ,(select count(*) from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'")',C_NEWLINE,
                             '       ,(select count(*) from "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'")',C_NEWLINE,
                             '       ,(select count(*) from SOURCE_HASH_TABLE T1 LEFT JOIN TARGET_HASH_TABLE T2 USING (HASH,CNT) where T2.HASH is null)',C_NEWLINE,
                             '       ,(select count(*) from TARGET_HASH_TABLE T1 LEFT JOIN SOURCE_HASH_TABLE T2 USING (HASH,CNT) where T2.HASH is null)');   
    
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