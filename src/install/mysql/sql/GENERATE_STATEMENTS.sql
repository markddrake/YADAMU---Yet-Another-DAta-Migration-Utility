SET SESSION SQL_MODE=ANSI_QUOTES;
--
DROP PROCEDURE IF EXISTS GENERATE_STATEMENTS;
--
DELIMITER $$
--
CREATE PROCEDURE GENERATE_STATEMENTS(P_METADATA JSON, P_TYPE_MAPPINGS JSON, P_TARGET_SCHEMA VARCHAR(128), P_OPTIONS JSON, OUT P_RESULTS JSON)
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
  
  SET V_SPATIAL_FORMAT = JSON_VALUE(P_OPTIONS, '$.spatialFormat');
  SET V_CIRCLE_FORMAT = JSON_VALUE(P_OPTIONS, '$.circleFormat');
 
  CALL SET_VENDOR_TYPE_MAPPINGS (P_TYPE_MAPPINGS);
  
  SET NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    FETCH TABLE_METADATA INTO V_VENDOR, V_TABLE_NAME, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;
	
	select V_VENDOR, P_TARGET_SCHEMA, V_TABLE_NAME, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY, V_SPATIAL_FORMAT, V_CIRCLE_FORMAT;
    
    CALL GENERATE_SQL(V_VENDOR, P_TARGET_SCHEMA, V_TABLE_NAME, V_COLUMN_NAME_ARRAY, V_DATA_TYPE_ARRAY, V_SIZE_CONSTRAINT_ARRAY, V_SPATIAL_FORMAT, V_CIRCLE_FORMAT, V_TABLE_INFO);
    SET P_RESULTS = JSON_INSERT(P_RESULTS,concat('$."',V_TABLE_NAME,'"'),V_TABLE_INFO);
     
  END LOOP;
 
  CLOSE TABLE_METADATA;
end;
$$
--
DELIMITER ;
--
