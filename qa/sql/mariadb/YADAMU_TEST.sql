/*
**
** MySQL/MariaDB COMPARE_SCHEMAS Function.
**
*/
SET SESSION SQL_MODE=ANSI_QUOTES;
--
SET COLLATION_CONNECTION = @@COLLATION_SERVER;
--
DROP FUNCTION IF EXISTS FUDGE_COORDINATE;
--
DELIMITER $$
--
CREATE FUNCTION FUDGE_COORDINATE(P_COORDINATE DOUBLE PRECISION,P_SPATIAL_PRECISION int)
RETURNS DOUBLE PRECISION DETERMINISTIC
BEGIN
  DECLARE DRIFT           DOUBLE PRECISION;
  DECLARE RESULT          DOUBLE PRECISION;
  DECLARE FUDGE_FACTOR    DOUBLE PRECISION DEFAULT 5e-12;
  
  SET DRIFT = round((P_COORDINATE-truncate(P_COORDINATE,10))*1e10);
  case
    when (DRIFT = -1) then
      SET RESULT = P_COORDINATE - FUDGE_FACTOR;
    when (DRIFT = 1) then
      SET RESULT = P_COORDINATE + FUDGE_FACTOR;
    else
      SET RESULT = P_COORDINATE;
   end case;
   SET RESULT = truncate(RESULT,10);
   SET RESULT = truncate(RESULT,P_SPATIAL_PRECISION);
   return RESULT;
END
$$
--
DELIMITER ;
--
DROP FUNCTION IF EXISTS ROUND_GEOMETRY_WKT;
--
DELIMITER $$
--
CREATE FUNCTION ROUND_GEOMETRY_WKT(P_GEOMETRY GEOMETRY,P_SPATIAL_PRECISION int)
RETURNS MEDIUMTEXT DETERMINISTIC
BEGIN
  DECLARE POINT_SEPERATOR   CHAR(1);
  DECLARE POINT_NUMBER      INT;
  DECLARE POINT_COUNT       INT;
  
  DECLARE RING_SEPERATOR    CHAR(1);
  DECLARE RING_NUMBER       INT;
  DECLARE RING_COUNT        INT;
  
  DECLARE POLYGON_SEPERATOR CHAR(1);
  DECLARE POLYGON_NUMBER    INT;
  DECLARE POLYGON_COUNT     INT;

  DECLARE X                 DOUBLE PRECISION;
  DECLARE Y                 DOUBLE PRECISION;

  DECLARE RING              GEOMETRY;
  DECLARE POINT             GEOMETRY;
  DECLARE POLYGON           GEOMETRY;
  DECLARE MULTIPOLYGON      GEOMETRY;
  
  DECLARE WKT               MEDIUMTEXT;
  
  SET WKT = ST_GeometryType(P_GEOMETRY) ;

  if (ST_GeometryType(P_GEOMETRY) = 'POINT') then    
    SET X = FUDGE_COORDINATE(ST_X(P_GEOMETRY),P_SPATIAL_PRECISION);
    SET Y = FUDGE_COORDINATE(ST_Y(P_GEOMETRY),P_SPATIAL_PRECISION);
    SET WKT = concat(WKT,'(',X,' ',Y,')');
    return WKT;
  end if;
  
  -- Iterative the component geograpjys and points for LINE, POLYGON, MULTIPOLYGON etc

  if (ST_GeometryType(P_GEOMETRY) = 'POLYGON') then   
    SET WKT = concat(WKT,'(');
    SET RING = ST_ExteriorRing(P_GEOMETRY);
    SET POINT_NUMBER = 0;
    SET POINT_SEPERATOR = '';
    SET POINT_COUNT = ST_NumPoints(RING);
    SET WKT = concat(WKT,'(');
    while (POINT_NUMBER < POINT_COUNT) do
      SET POINT_NUMBER = POINT_NUMBER + 1;
      SET POINT = ST_PointN(RING,POINT_NUMBER);
      SET X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
      SET Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
      SET WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
      SET POINT_SEPERATOR = ',';
    end while;
    SET WKT = concat(WKT,')');

    SET RING_SEPERATOR = ',';
    SET RING_NUMBER = 0;
    SET RING_COUNT =  ST_NumInteriorRings(P_GEOMETRY);
    while (RING_NUMBER  < RING_COUNT) do
      SET WKT = concat(WKT,RING_SEPERATOR,'(');
      SET RING_NUMBER = RING_NUMBER + 1;
      SET RING = ST_InteriorRingN(P_GEOMETRY,RING_NUMBER);
      SET POINT_NUMBER = 0;
      SET POINT_SEPERATOR = '';
      SET POINT_COUNT = ST_NumPoints(RING);
      while (POINT_NUMBER < POINT_COUNT) do
        SET POINT_NUMBER = POINT_NUMBER + 1;
        SET POINT = ST_PointN(RING,POINT_NUMBER);
        SET X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
        SET Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
        SET WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
        SET POINT_SEPERATOR = ',';
      end while;
      SET WKT = concat(WKT,')');  
    end while;
    SET WKT = concat(WKT,')');  
    return WKT;   
  end if;

  if (ST_GeometryType(P_GEOMETRY) = 'MULTIPOLYGON') then   
    SET WKT = concat(WKT,'(');
    SET POLYGON_NUMBER = 0;
    SET POLYGON_SEPERATOR = '';
    SET POLYGON_COUNT = ST_NumGeometries(P_GEOMETRY);
    while (POLYGON_NUMBER  < POLYGON_COUNT) do
      SET WKT = concat(WKT,POLYGON_SEPERATOR,'(');
      set POLYGON_SEPERATOR = ',';
      SET POLYGON_NUMBER = POLYGON_NUMBER + 1;
      SET POLYGON = ST_GeometryN(P_GEOMETRY,POLYGON_NUMBER);
      SET RING = ST_ExteriorRing(POLYGON);
      SET POINT_NUMBER = 0;
      SET POINT_SEPERATOR = '';
      SET POINT_COUNT = ST_NumPoints(RING);
      SET WKT = concat(WKT,'(');
      while (POINT_NUMBER < POINT_COUNT) do
        SET POINT_NUMBER = POINT_NUMBER + 1;
        SET POINT = ST_PointN(RING,POINT_NUMBER);
        SET X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
        SET Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
        SET WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
        SET POINT_SEPERATOR = ',';
      end while;
      SET WKT = concat(WKT,')');
      SET RING_SEPERATOR = ',';
      SET RING_NUMBER = 0;
      SET RING_COUNT =  ST_NumInteriorRings(POLYGON);
      while (RING_NUMBER  < RING_COUNT) do
        SET WKT = concat(WKT,RING_SEPERATOR,'(');
        SET RING_NUMBER = RING_NUMBER + 1;
        SET RING = ST_InteriorRingN(POLYGON,RING_NUMBER);
        SET POINT_NUMBER = 0;
        SET POINT_SEPERATOR = '';
        SET POINT_COUNT = ST_NumPoints(RING);
        while (POINT_NUMBER < POINT_COUNT) do
          SET POINT_NUMBER = POINT_NUMBER + 1;
          SET POINT = ST_PointN(RING,POINT_NUMBER);
          SET X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
          SET Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
          SET WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
          SET POINT_SEPERATOR = ',';
        end while;
        SET WKT = concat(WKT,')');
      end while;
      SET WKT = concat(WKT,')');      
    end while;
    SET WKT = concat(WKT,')');      
    return WKT;      
  end if;     

  return ST_AsText(P_GEOMETRY);
END
$$
--
DELIMITER ;
--
DROP FUNCTION IF EXISTS ROUND_GEOMETRY;
--
DELIMITER $$
--
CREATE FUNCTION ROUND_GEOMETRY(P_GEOMETRY GEOMETRY,P_SPATIAL_PRECISION int)
RETURNS GEOMETRY DETERMINISTIC
BEGIN
  return ST_GeomFromText(ROUND_GEOMETRY_WKT(P_GEOMETRY,P_SPATIAL_PRECISION));      
END
$$
--
DELIMITER ;
--
DROP FUNCTION IF EXISTS  POINTS_FROM_GEOMETRY;
--
DELIMITER $$
--
CREATE FUNCTION POINTS_FROM_GEOMETRY(P_ID INT, P_GEOMETRY1 GEOMETRY,P_GEOMETRY2 GEOMETRY)
RETURNS INT DETERMINISTIC
BEGIN
  DECLARE POINT_NUMBER      INT;
  DECLARE POINT_COUNT       INT;
  
  DECLARE RING_NUMBER       INT;
  DECLARE RING_COUNT        INT;
  
  DECLARE POLYGON_NUMBER    INT;
  DECLARE POLYGON_COUNT     INT;

  DECLARE X                 VARCHAR(32);
  DECLARE Y                 VARCHAR(32);

  DECLARE RING1             GEOMETRY;
  DECLARE POINT1            GEOMETRY;
  DECLARE POLYGON1          GEOMETRY;
  DECLARE RING2             GEOMETRY;
  DECLARE POINT2            GEOMETRY;
  DECLARE POLYGON2          GEOMETRY;
  
  DECLARE POINT_INDEX       INT DEFAULT 1;
  
  DECLARE RESULTS MEDIUMTEXT;

  create temporary table if not exists GEOMETRY_POINTS_TABLE (
    ID     INT,
    IDX    INT,
    X1     DOUBLE PRECISION,
    Y1     DOUBLE PRECISION,
    X2     DOUBLE PRECISION,
    Y2     DOUBLE PRECISION
  ) ENGINE=MEMORY;
  
  case 
    when (ST_GeometryType(P_GEOMETRY1) = 'POINT') then    
      INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(P_GEOMETRY1), ST_Y(P_GEOMETRY1), ST_X(P_GEOMETRY2), ST_Y(P_GEOMETRY2));
	  RETURN POINT_INDEX;
    when (ST_GeometryType(P_GEOMETRY1) = 'POLYGON') then   
    begin
      SET RING1 = ST_ExteriorRing(P_GEOMETRY1);
      SET POINT_NUMBER = 0;
      SET POINT_COUNT = ST_NumPoints(RING1);
      while (POINT_NUMBER < POINT_COUNT) do
        SET POINT_NUMBER = POINT_NUMBER + 1;
        SET POINT1 = ST_PointN(RING1,POINT_NUMBER);
        SET POINT2 = ST_PointN(RING2,POINT_NUMBER);
        INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
         SET POINT_INDEX = POINT_INDEX + 1;
      end while;
     
      SET RING_NUMBER = 0;
      SET RING_COUNT =  ST_NumInteriorRings(P_GEOMETRY1);
      while (RING_NUMBER  < RING_COUNT) do
        SET RING_NUMBER = RING_NUMBER + 1;
        SET RING1 = ST_InteriorRingN(P_GEOMETRY1,RING_NUMBER);
        SET RING2 = ST_InteriorRingN(P_GEOMETRY2,RING_NUMBER);
        SET POINT_NUMBER = 0;
        SET POINT_COUNT = ST_NumPoints(RING1);
        while (POINT_NUMBER < POINT_COUNT) do
          SET POINT_NUMBER = POINT_NUMBER + 1;
          SET POINT1 = ST_PointN(RING1,POINT_NUMBER);
          SET POINT2 = ST_PointN(RING2,POINT_NUMBER);
          INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
          SET POINT_INDEX = POINT_INDEX + 1;
        end while;
      end while;
	  RETURN POINT_INDEX;
	end; 
    when(ST_GeometryType(P_GEOMETRY1) = 'MULTIPOLYGON') then   
    begin
      SET POLYGON_NUMBER = 0;
      SET POLYGON_COUNT = ST_NumGeometries(P_GEOMETRY1);
      while (POLYGON_NUMBER  < POLYGON_COUNT) do
        SET POLYGON_NUMBER = POLYGON_NUMBER + 1;
        SET POLYGON1 = ST_GeometryN(P_GEOMETRY1,POLYGON_NUMBER);
        SET POLYGON2 = ST_GeometryN(P_GEOMETRY2,POLYGON_NUMBER);
        SET RING1 = ST_ExteriorRing(POLYGON1);
        SET RING2 = ST_ExteriorRing(POLYGON2);
        SET POINT_NUMBER = 0;
        SET POINT_COUNT = ST_NumPoints(RING1);
        while (POINT_NUMBER < POINT_COUNT) do
          SET POINT_NUMBER = POINT_NUMBER + 1;
          SET POINT1 = ST_PointN(RING1,POINT_NUMBER);
          SET POINT2 = ST_PointN(RING2,POINT_NUMBER);
          INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
          SET POINT_INDEX = POINT_INDEX + 1;
        end while;
        SET RING_NUMBER = 0;
        SET RING_COUNT =  ST_NumInteriorRings(POLYGON1);
        while (RING_NUMBER  < RING_COUNT) do
          SET RING_NUMBER = RING_NUMBER + 1;
          SET RING1 = ST_InteriorRingN(POLYGON1,RING_NUMBER);
          SET RING2 = ST_InteriorRingN(POLYGON2,RING_NUMBER);
          SET POINT_NUMBER = 0;
          SET POINT_COUNT = ST_NumPoints(RING1);
          while (POINT_NUMBER < POINT_COUNT) do
            SET POINT_NUMBER = POINT_NUMBER + 1;
            SET POINT1 = ST_PointN(RING1,POINT_NUMBER);
            SET POINT2 = ST_PointN(RING2,POINT_NUMBER);
            INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
            SET POINT_INDEX = POINT_INDEX + 1;
          end while;
        end while;
      end while;
	  RETURN POINT_INDEX;
    end;
    else
	  RETURN -1;
    begin
	end;
  end case;     
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
  declare TABLE_NOT_FOUND CONDITION for 1146; 

  declare C_NEWLINE             VARCHAR(1) DEFAULT CHAR(32);
  
  declare NO_MORE_ROWS          INT DEFAULT FALSE;
  declare MISSING_TABLE         INT DEFAULT FALSE;
  declare V_TABLE_NAME          VARCHAR(128);
  declare V_SOURCE_COLUMN_LIST  TEXT;
  declare V_TARGET_COLUMN_LIST  TEXT;
  declare V_STATEMENT           TEXT;
  declare V_COUNT_STATEMENT     TEXT;
  
  
  declare MISSING_ROWS INT;
  declare EXTRA_ROWS INT;
  declare ROW_COUNT INT;
  
  declare V_SQLSTATE         INT;
  declare V_SQLERRM          TEXT;
  
  declare TABLE_METADATA 
  CURSOR FOR 
  select c.table_name "TABLE_NAME"
        ,group_concat(case 
                        when data_type in ('geometry') then
                          case
                            when P_SPATIAL_PRECISION = 18 then
                              concat('"',column_name,'"') 
                            else                            
                              concat('ROUND_GEOMETRY(',column_name,',',P_SPATIAL_PRECISION,')')
                          end
                        when data_type in ('blob', 'varbinary', 'binary') then
                          concat('hex("',column_name,'")') 
                        when data_type in ('set') then
						  -- Set is stored as a JSON_ARRAY in the target...
                          concat('json_compact(concat(''["'',replace("',column_name,'",'','',''","''),''"]''))') 
                        when data_type in ('varchar','text','mediumtext','longtext') then
                          case
                            when P_MAP_EMPTY_STRING_TO_NULL then
                              concat('case when "',column_name,'" = '''' then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        else concat('"',column_name,'"') 
                      end
					  order by ordinal_position separator ',')  "SOURCE_COLUMNS"
        ,group_concat(case 
                        when data_type in ('geometry') then
                          case
                            when P_SPATIAL_PRECISION = 18 then
                              concat('"',column_name,'"') 
                            else                            
                              concat('ROUND_GEOMETRY(',column_name,',',P_SPATIAL_PRECISION,')')
                          end
                        when data_type in ('blob', 'varbinary', 'binary') then
                          concat('hex("',column_name,'")') 
                        when data_type in ('set') then
						  -- Set is stored as a JSON_ARRAY in the target...
                          concat('json_compact("',column_name,'")') 
                        when data_type in ('varchar','text','mediumtext','longtext') then
                          case
                            when P_MAP_EMPTY_STRING_TO_NULL then
                              concat('case when "',column_name,'" = '''' then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        else concat('"',column_name,'"') 
                      end 
                      order by ordinal_position separator ',')  "TARGET_COLUMNS"
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


  declare CONTINUE HANDLER FOR NOT FOUND set NO_MORE_ROWS = TRUE;

  declare CONTINUE HANDLER FOR TABLE_NOT_FOUND
  begin 
    get diagnostics CONDITION 1
       V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT; 
    set V_COUNT_STATEMENT = concat('select count(*) into @ROW_COUNT from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'"');
    set @STATEMENT = V_COUNT_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
    
    insert into SCHEMA_COMPARE_RESULTS values (P_SOURCE_SCHEMA,P_TARGET_SCHEMA,V_TABLE_NAME,@ROW_COUNT,-1,@ROW_COUNT,-1,V_SQLERRM);
    set MISSING_TABLE = true;
  end;  
 
  declare CONTINUE HANDLER FOR SQLEXCEPTION
  begin 
    get diagnostics CONDITION 1
       V_SQLSTATE = RETURNED_SQLSTATE, V_SQLERRM = MESSAGE_TEXT; 
    insert into SCHEMA_COMPARE_RESULTS values (P_SOURCE_SCHEMA,P_TARGET_SCHEMA,V_TABLE_NAME,-1,-1,-1,-1,V_SQLERRM);
  end;  

  set SESSION SQL_MODE=ANSI_QUOTES;
  set SESSION group_concat_max_len = 131072;
  set max_heap_table_size = 1 * 1024 *1024 *1024;
  create temporary table if not exists SCHEMA_COMPARE_RESULTS (
    SOURCE_SCHEMA    VARCHAR(128)
   ,TARGET_SCHEMA    VARCHAR(128)
   ,TABLE_NAME       VARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSING_ROWS     INT
   ,EXTRA_ROWS       INT
   ,NOTES            VARCHAR(512)
  );
  
  create temporary table if not exists SOURCE_HASH_TABLE (
    HASH    BINARY(32)
  ) ENGINE=MEMORY;

  create temporary table if not exists TARGET_HASH_TABLE (
    HASH    BINARY(32)
  ) ENGINE=MEMORY;
  
  TRUNCATE TABLE SCHEMA_COMPARE_RESULTS;
  COMMIT;
  
  set NO_MORE_ROWS = FALSE;
  OPEN TABLE_METADATA;
    
  PROCESS_TABLE : LOOP
    SET MISSING_TABLE = false;
    FETCH TABLE_METADATA INTO V_TABLE_NAME, V_SOURCE_COLUMN_LIST, V_TARGET_COLUMN_LIST;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;

    TRUNCATE TABLE SOURCE_HASH_TABLE;
    TRUNCATE TABLE TARGET_HASH_TABLE;
	
	SET V_STATEMENT = CONCAT('insert into SOURCE_HASH_TABLE select UNHEX(SHA2(JSON_ARRAY(',V_SOURCE_COLUMN_LIST,'),256)) HASH from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'"');
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
    
	SET V_STATEMENT = CONCAT('insert into TARGET_HASH_TABLE select UNHEX(SHA2(JSON_ARRAY(',V_TARGET_COLUMN_LIST,'),256)) HASH from "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'"');
    SET @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
	IF (MISSING_TABLE) THEN 
	  ITERATE PROCESS_TABLE;
	END IF;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;

    set V_STATEMENT = CONCAT('insert into SCHEMA_COMPARE_RESULTS ',C_NEWLINE,
	                         'with ',C_NEWLINE,
							 'MISSING_ROWS as (',C_NEWLINE,
							 'select HASH from SOURCE_HASH_TABLE where (HASH) not in (select HASH from TARGET_HASH_TABLE)',C_NEWLINE,
							 '),',C_NEWLINE,
							 'EXTRA_ROWS as (',C_NEWLINE,
							 'select HASH from TARGET_HASH_TABLE where (HASH) not in (select HASH from SOURCE_HASH_TABLE)',C_NEWLINE,
							 ')',C_NEWLINE,							 
                             ' select ''',P_SOURCE_SCHEMA,''' ',C_NEWLINE,
                             '       ,''',P_TARGET_SCHEMA,''' ',C_NEWLINE,
                             '       ,''',V_TABLE_NAME,''' ',C_NEWLINE,
                             '       ,(select count(*) from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'")',C_NEWLINE,
                             '       ,(select count(*) from "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'")',C_NEWLINE,
                             '       ,(select count(*) from MISSING_ROWS) ',C_NEWLINE,
                             '       ,(select count(*) from EXTRA_ROWS) ',C_NEWLINE,
                             '       ,NULL');   
    
    set @STATEMENT = V_STATEMENT;
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
--
