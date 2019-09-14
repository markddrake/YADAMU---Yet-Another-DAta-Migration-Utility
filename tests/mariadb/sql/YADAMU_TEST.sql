/*
**
** MariaDB COMPARE_SCHEMAS Function.
**
*/
SET SESSION SQL_MODE=ANSI_QUOTES;
--
DROP FUNCTION IF EXISTS SET_GEOMETRY_PRECISION;
--
DELIMITER $$
--
CREATE FUNCTION SET_GEOMETRY_PRECISION(P_GEOMETRY GEOMETRY,P_SPATIAL_PRECISION int)
RETURNS GEOMETRY DETERMINISTIC
BEGIN
  DECLARE POINT       GEOMETRY;
  DECLARE X           DOUBLE;
  DECLARE Y           DOUBLE;
  if ST_GeometryType(P_GEOMETRY) = 'POINT' then
      
    SET X = ROUND(ST_X(P_GEOMETRY),P_SPATIAL_PRECISION);
    SET Y = ROUND(ST_Y(P_GEOMETRY),P_SPATIAL_PRECISION);
    SET POINT = ST_GeomFromText(concat('POINT(',X,' ',Y,')'));      
    return POINT;
  end if;
  -- Iterative the component geometry and points
  return P_GEOMETRY;
END
$$
--
DELIMITER ;
--
DROP PROCEDURE IF EXISTS COMPARE_SCHEMAS;
--
DELIMITER $$
--
CREATE PROCEDURE COMPARE_SCHEMAS(P_SOURCE_SCHEMA VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128), P_MAP_EMPTY_STRING_TO_NULL BOOLEAN, P_SPATIAL_PRECISION INT)
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
        ,group_concat(case 
                        when data_type in ('geometry') then
                          case
                            when P_SPATIAL_PRECISION = 18 then
                              concat('"',column_name,'"') 
                            else                            
                              concat('SET_GEOMETRY_PRECISION(',column_name,',',P_SPATIAL_PRECISION,')')
                          end                                                           
                        when data_type in ('blob', 'varbinary', 'binary') then
                          concat('hex("',column_name,'")') 
                        when data_type in ('varchar','text','mediumtext','longtext') then
                          case
                            when P_MAP_EMPTY_STRING_TO_NULL then
                              concat('case when "',column_name,'" = '''' then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        else concat('"',column_name,'"') 
                      end 
                      order by ordinal_position separator ',')  "COLUMNS"
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
  
  SET SESSION SQL_MODE=ANSI_QUOTES;
  SET SESSION group_concat_max_len = 131072;
  SET max_heap_table_size = 1 * 1024 *1024 *1024;
  create temporary table if not exists SCHEMA_COMPARE_RESULTS (
    SOURCE_SCHEMA    VARCHAR(128)
   ,TARGET_SCHEMA    VARCHAR(128)
   ,TABLE_NAME       VARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSING_ROWS     INT
   ,EXTRA_ROWS       INT
   ,SQLERRM          VARCHAR(512)
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
                             '       ,(select count(*) from TARGET_HASH_TABLE T1 LEFT JOIN SOURCE_HASH_TABLE T2 USING (HASH,CNT) where T2.HASH is null)',
                             '       ,NULL');   
    
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
     
  END LOOP;
 
  CLOSE TABLE_METADATA;

  -- Avoid Running out of OS Memory
  FLUSH TABLES;
end;
$$
--
DELIMITER ;