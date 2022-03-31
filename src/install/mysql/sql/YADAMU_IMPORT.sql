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