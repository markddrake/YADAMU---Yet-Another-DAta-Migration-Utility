/*
**
** MySQL/MariaDB COMPARE_SCHEMAS Function.
**
*/--
set SESSION SQL_MODE=ANSI_QUOTES;
--
set COLLATION_CONNECTION = @@COLLATION_SERVER;
--
DROP FUNCTION IF EXISTS ORDERED_JSON;
--
DROP FUNCTION IF EXISTS ORDER_JSON_DOCUMENT;
--
DROP FUNCTION IF EXISTS ORDER_JSON_OBJECT;
--
DROP FUNCTION IF EXISTS ORDER_JSON_ARRAY;
--
DROP PROCEDURE IF EXISTS ORDERED_JSON_IMPL;
--
DROP PROCEDURE IF EXISTS ORDER_JSON_VALUES;
--
DROP PROCEDURE IF EXISTS YADAMU_JSON_OBJECT_NORMALIZE;
--
DROP PROCEDURE IF EXISTS YADAMU_JSON_ARRAY_NORMALIZE;
--
DROP FUNCTION IF EXISTS YADAMU_JSON_NORMALIZE;
--
DROP FUNCTION IF EXISTS FUDGE_COORDINATE;
--
DELIMITER $$
--
CREATE FUNCTION FUDGE_COORDINATE(P_COORDINATE DOUBLE PRECISION,P_SPATIAL_PRECISION int)
RETURNS DOUBLE PRECISION DETERMINISTIC
BEGIN
  declare DRIFT           DOUBLE PRECISION;
  declare RESULT          DOUBLE PRECISION;
  declare FUDGE_FACTOR    DOUBLE PRECISION DEFAULT 5e-12;
  
  if (P_SPATIAL_PRECISION = -18) then
    if (P_COORDINATE = 180.00000000000003) then
	 -- Postgres Specific Fix.
      set RESULT = 180.0;
	else 
	  set RESULT  = P_COORDINATE;
	end if;
  else  
    set DRIFT = round((P_COORDINATE-truncate(P_COORDINATE,10))*1e10);
    case
      when (DRIFT = -1) then
        set RESULT = P_COORDINATE - FUDGE_FACTOR;
      when (DRIFT = 1) then
        set RESULT = P_COORDINATE + FUDGE_FACTOR;
      else
        set RESULT = P_COORDINATE;
     end case;
     set RESULT = truncate(RESULT,10);
     set RESULT = truncate(RESULT,P_SPATIAL_PRECISION);
  end if;
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
  declare POINT_SEPERATOR   CHAR(1);
  declare POINT_NUMBER      INT;
  declare POINT_COUNT       INT;
  
  declare RING_SEPERATOR    CHAR(1);
  declare RING_NUMBER       INT;
  declare RING_COUNT        INT;
  
  declare POLYGON_SEPERATOR CHAR(1);
  declare POLYGON_NUMBER    INT;
  declare POLYGON_COUNT     INT;

  declare X                 DOUBLE PRECISION;
  declare Y                 DOUBLE PRECISION;

  declare RING              GEOMETRY;
  declare POINT             GEOMETRY;
  declare POLYGON           GEOMETRY;
  declare MULTIPOLYGON      GEOMETRY;
  
  declare WKT               MEDIUMTEXT;
  
  set WKT = ST_GeometryType(P_GEOMETRY) ;

  if (ST_GeometryType(P_GEOMETRY) = 'POINT') then    
    set X = FUDGE_COORDINATE(ST_X(P_GEOMETRY),P_SPATIAL_PRECISION);
    set Y = FUDGE_COORDINATE(ST_Y(P_GEOMETRY),P_SPATIAL_PRECISION);
    set WKT = concat(WKT,'(',X,' ',Y,')');
    return WKT;
  end if;
  
 -- Iterative the component geograpjys and points for LINE, POLYGON, MULTIPOLYGON etc

  if (ST_GeometryType(P_GEOMETRY) = 'POLYGON') then   
    set WKT = concat(WKT,'(');
    set RING = ST_ExteriorRing(P_GEOMETRY);
    set POINT_NUMBER = 0;
    set POINT_SEPERATOR = '';
    set POINT_COUNT = ST_NumPoints(RING);
    set WKT = concat(WKT,'(');
    while (POINT_NUMBER < POINT_COUNT) do
      set POINT_NUMBER = POINT_NUMBER + 1;
      set POINT = ST_PointN(RING,POINT_NUMBER);
      set X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
      set Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
      set WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
      set POINT_SEPERATOR = ',';
    end while;
    set WKT = concat(WKT,')');

    set RING_SEPERATOR = ',';
    set RING_NUMBER = 0;
    set RING_COUNT =  ST_NumInteriorRings(P_GEOMETRY);
    while (RING_NUMBER  < RING_COUNT) do
      set WKT = concat(WKT,RING_SEPERATOR,'(');
      set RING_NUMBER = RING_NUMBER + 1;
      set RING = ST_InteriorRingN(P_GEOMETRY,RING_NUMBER);
      set POINT_NUMBER = 0;
      set POINT_SEPERATOR = '';
      set POINT_COUNT = ST_NumPoints(RING);
      while (POINT_NUMBER < POINT_COUNT) do
        set POINT_NUMBER = POINT_NUMBER + 1;
        set POINT = ST_PointN(RING,POINT_NUMBER);
        set X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
        set Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
        set WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
        set POINT_SEPERATOR = ',';
      end while;
      set WKT = concat(WKT,')');  
    end while;
    set WKT = concat(WKT,')');  
    return WKT;   
  end if;

  if (ST_GeometryType(P_GEOMETRY) = 'MULTIPOLYGON') then   
    set WKT = concat(WKT,'(');
    set POLYGON_NUMBER = 0;
    set POLYGON_SEPERATOR = '';
    set POLYGON_COUNT = ST_NumGeometries(P_GEOMETRY);
    while (POLYGON_NUMBER  < POLYGON_COUNT) do
      set WKT = concat(WKT,POLYGON_SEPERATOR,'(');
      set POLYGON_SEPERATOR = ',';
      set POLYGON_NUMBER = POLYGON_NUMBER + 1;
      set POLYGON = ST_GeometryN(P_GEOMETRY,POLYGON_NUMBER);
      set RING = ST_ExteriorRing(POLYGON);
      set POINT_NUMBER = 0;
      set POINT_SEPERATOR = '';
      set POINT_COUNT = ST_NumPoints(RING);
      set WKT = concat(WKT,'(');
      while (POINT_NUMBER < POINT_COUNT) do
        set POINT_NUMBER = POINT_NUMBER + 1;
        set POINT = ST_PointN(RING,POINT_NUMBER);
        set X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
        set Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
        set WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
        set POINT_SEPERATOR = ',';
      end while;
      set WKT = concat(WKT,')');
      set RING_SEPERATOR = ',';
      set RING_NUMBER = 0;
      set RING_COUNT =  ST_NumInteriorRings(POLYGON);
      while (RING_NUMBER  < RING_COUNT) do
        set WKT = concat(WKT,RING_SEPERATOR,'(');
        set RING_NUMBER = RING_NUMBER + 1;
        set RING = ST_InteriorRingN(POLYGON,RING_NUMBER);
        set POINT_NUMBER = 0;
        set POINT_SEPERATOR = '';
        set POINT_COUNT = ST_NumPoints(RING);
        while (POINT_NUMBER < POINT_COUNT) do
          set POINT_NUMBER = POINT_NUMBER + 1;
          set POINT = ST_PointN(RING,POINT_NUMBER);
          set X = FUDGE_COORDINATE(ST_X(POINT),P_SPATIAL_PRECISION);
          set Y = FUDGE_COORDINATE(ST_Y(POINT),P_SPATIAL_PRECISION);
          set WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
          set POINT_SEPERATOR = ',';
        end while;
        set WKT = concat(WKT,')');
      end while;
      set WKT = concat(WKT,')');      
    end while;
    set WKT = concat(WKT,')');      
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
  declare POINT_NUMBER      INT;
  declare POINT_COUNT       INT;
  
  declare RING_NUMBER       INT;
  declare RING_COUNT        INT;
  
  declare POLYGON_NUMBER    INT;
  declare POLYGON_COUNT     INT;

  declare X                 VARCHAR(32);
  declare Y                 VARCHAR(32);

  declare RING1             GEOMETRY;
  declare POINT1            GEOMETRY;
  declare POLYGON1          GEOMETRY;
  declare RING2             GEOMETRY;
  declare POINT2            GEOMETRY;
  declare POLYGON2          GEOMETRY;
  
  declare POINT_INDEX       INT DEFAULT 1;
  
  declare RESULTS MEDIUMTEXT;

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
      set RING1 = ST_ExteriorRing(P_GEOMETRY1);
      set POINT_NUMBER = 0;
      set POINT_COUNT = ST_NumPoints(RING1);
      while (POINT_NUMBER < POINT_COUNT) do
        set POINT_NUMBER = POINT_NUMBER + 1;
        set POINT1 = ST_PointN(RING1,POINT_NUMBER);
        set POINT2 = ST_PointN(RING2,POINT_NUMBER);
        INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
         set POINT_INDEX = POINT_INDEX + 1;
      end while;
     
      set RING_NUMBER = 0;
      set RING_COUNT =  ST_NumInteriorRings(P_GEOMETRY1);
      while (RING_NUMBER  < RING_COUNT) do
        set RING_NUMBER = RING_NUMBER + 1;
        set RING1 = ST_InteriorRingN(P_GEOMETRY1,RING_NUMBER);
        set RING2 = ST_InteriorRingN(P_GEOMETRY2,RING_NUMBER);
        set POINT_NUMBER = 0;
        set POINT_COUNT = ST_NumPoints(RING1);
        while (POINT_NUMBER < POINT_COUNT) do
          set POINT_NUMBER = POINT_NUMBER + 1;
          set POINT1 = ST_PointN(RING1,POINT_NUMBER);
          set POINT2 = ST_PointN(RING2,POINT_NUMBER);
          INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
          set POINT_INDEX = POINT_INDEX + 1;
        end while;
      end while;
	  RETURN POINT_INDEX;
	end; 
    when(ST_GeometryType(P_GEOMETRY1) = 'MULTIPOLYGON') then   
    begin
      set POLYGON_NUMBER = 0;
      set POLYGON_COUNT = ST_NumGeometries(P_GEOMETRY1);
      while (POLYGON_NUMBER  < POLYGON_COUNT) do
        set POLYGON_NUMBER = POLYGON_NUMBER + 1;
        set POLYGON1 = ST_GeometryN(P_GEOMETRY1,POLYGON_NUMBER);
        set POLYGON2 = ST_GeometryN(P_GEOMETRY2,POLYGON_NUMBER);
        set RING1 = ST_ExteriorRing(POLYGON1);
        set RING2 = ST_ExteriorRing(POLYGON2);
        set POINT_NUMBER = 0;
        set POINT_COUNT = ST_NumPoints(RING1);
        while (POINT_NUMBER < POINT_COUNT) do
          set POINT_NUMBER = POINT_NUMBER + 1;
          set POINT1 = ST_PointN(RING1,POINT_NUMBER);
          set POINT2 = ST_PointN(RING2,POINT_NUMBER);
          INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
          set POINT_INDEX = POINT_INDEX + 1;
        end while;
        set RING_NUMBER = 0;
        set RING_COUNT =  ST_NumInteriorRings(POLYGON1);
        while (RING_NUMBER  < RING_COUNT) do
          set RING_NUMBER = RING_NUMBER + 1;
          set RING1 = ST_InteriorRingN(POLYGON1,RING_NUMBER);
          set RING2 = ST_InteriorRingN(POLYGON2,RING_NUMBER);
          set POINT_NUMBER = 0;
          set POINT_COUNT = ST_NumPoints(RING1);
          while (POINT_NUMBER < POINT_COUNT) do
            set POINT_NUMBER = POINT_NUMBER + 1;
            set POINT1 = ST_PointN(RING1,POINT_NUMBER);
            set POINT2 = ST_PointN(RING2,POINT_NUMBER);
            INSERT INTO GEOMETRY_POINTS_TABLE values (P_ID, POINT_INDEX, ST_X(POINT1),ST_Y(POINT1), ST_X(POINT2),ST_Y(POINT2));
            set POINT_INDEX = POINT_INDEX + 1;
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
CREATE PROCEDURE COMPARE_SCHEMAS(P_SOURCE_SCHEMA VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128), P_COMPARE_RULES JSON)
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
  
  declare V_EMPTY_STRING_IS_NULL     BOOLEAN    DEFAULT JSON_VALUE(P_COMPARE_RULES,'$.emptyStringIsNull');
  declare V_MIN_BIGINT_IS_NULL       BOOLEAN    DEFAULT JSON_VALUE(P_COMPARE_RULES,'$.minBigIntIsNull');
  declare V_SPATIAL_PRECISION_VALUE  VARCHAR(4) DEFAULT JSON_VALUE(P_COMPARE_RULES,'$.spatialPrecision');
  declare V_DOUBLE_PRECISION_VALUE   VARCHAR(4) DEFAULT JSON_VALUE(P_COMPARE_RULES,'$.doublePrecision');
  declare V_NUMERIC_SCALE_VALUE      VARCHAR(4) DEFAULT JSON_VALUE(P_COMPARE_RULES,'$.numericScale');
  declare V_ORDERED_JSON             BOOLEAN    DEFAULT JSON_VALUE(P_COMPARE_RULES,'$.orderedJSON');
  
  declare V_SPATIAL_PRECISION       INT DEFAULT CASE WHEN V_SPATIAL_PRECISION_VALUE = 'null' THEN NULL ELSE CAST(V_SPATIAL_PRECISION_VALUE AS INT) END;
  declare V_DOUBLE_PRECISION        INT DEFAULT CASE WHEN V_DOUBLE_PRECISION_VALUE = 'null' THEN NULL ELSE CAST(V_DOUBLE_PRECISION_VALUE AS INT) END;
  declare V_NUMERIC_SCALE           INT DEFAULT CASE WHEN V_NUMERIC_SCALE_VALUE = 'null' THEN NULL ELSE CAST(V_NUMERIC_SCALE_VALUE AS INT) END;
  
  declare MISSING_ROWS INT;
  declare EXTRA_ROWS INT;
  declare ROW_COUNT INT;
  
  declare V_SQLSTATE         INT;
  declare V_SQLERRM          TEXT;

  -- Cannot use JSON_COMPACT to normalize JSON spacing because JSON_ARRAY(JSON_COMPACT(X)) apprears to normaized to X. 
  
  declare TABLE_METADATA 
  CURSOR FOR 
  select c.table_name "TABLE_NAME"
        ,group_concat(case 
                        when data_type in ('double') and (V_DOUBLE_PRECISION < 18) then
                          concat('round("',column_name,'",',V_DOUBLE_PRECISION,')') 
		                when ((data_type = 'longtext') and (check_clause is not null)) then
   				          concat('JSON_NORMALIZE("',column_name,'")')
                        when data_type in ('geometry') then
                          case
                            when V_SPATIAL_PRECISION = 18 then
                              concat('"',column_name,'"') 
                            else                            
                              concat('ROUND_GEOMETRY(',column_name,',',V_SPATIAL_PRECISION,')')
                          end
                        when data_type in ('blob', 'varbinary', 'binary') then
                          concat('hex("',column_name,'")') 
                        when data_type in ('set') then
						  -- Convert a set in the source table into a JSON_ARRAY. 
						  -- Replace commas with Double Quote, Comma, Double Quote and wrap with Brackets and Quotes
						  -- Use JSON_Extract to normalize JSON formatting
						  concat('JSON_EXTRACT(CONCAT(''["'',REPLACE("',column_name,'",'','',''","''),''"]''),''$'')') 
                        when data_type in ('varchar','text','mediumtext','longtext') then
                          case
                            when V_EMPTY_STRING_IS_NULL then
                              concat('case when "',column_name,'" = '''' then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        when data_type in ('bigint') then
                          case
                            when V_MIN_BIGINT_IS_NULL then
                              concat('case when "',column_name,'" = -9223372036854775808 then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        when data_type in ('decimal') then
                          case
                            when (V_NUMERIC_SCALE is not NULL) and (V_NUMERIC_SCALE < numeric_scale) then
                               concat('round("',column_name,'",',V_NUMERIC_SCALE,')')
                            else 
                              concat('"',column_name,'"') 
                          end 
                        else concat('"',column_name,'"') 
                      end
					  order by ordinal_position separator ',')  "SOURCE_COLUMNS"
        ,group_concat(case 
                        when data_type in ('double') and (V_DOUBLE_PRECISION < 18) then
                          concat('round("',column_name,'",',V_DOUBLE_PRECISION,')') 
		                when ((data_type = 'longtext') and (check_clause is not null)) then
   				          concat('JSON_NORMALIZE("',column_name,'")')
                        when data_type in ('geometry') then
                          case
                            when V_SPATIAL_PRECISION = 18 then
                              concat('"',column_name,'"') 
                            else                            
                              concat('ROUND_GEOMETRY(',column_name,',',V_SPATIAL_PRECISION,')')
                          end
                        when data_type in ('blob', 'varbinary', 'binary') then
                          concat('hex("',column_name,'")') 
                        when data_type in ('set') then
						  -- A Set in the source schema will transformed into a JSON_ARRAY in the target database and then stored as JSON in the compare schema.
						  -- Use JSON_EXTRACT to normalize spacing of JSON.
						  -- There is no need to order the JSON as a set is mapped to to an array of strings
                          concat('JSON_EXTRACT("',column_name,'",''$'')') 
                        when data_type in ('varchar','text','mediumtext','longtext') then
                          case
                            when V_EMPTY_STRING_IS_NULL then
                              concat('case when "',column_name,'" = '''' then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        when data_type in ('bigint') then
                          case
                            when V_MIN_BIGINT_IS_NULL then
                              concat('case when "',column_name,'" = -9223372036854775808 then NULL else "',column_name,'" end') 
                            else 
                              concat('"',column_name,'"') 
                          end 
                        when data_type in ('decimal') then
                          case
                            when (V_NUMERIC_SCALE is not NULL) and (V_NUMERIC_SCALE < numeric_scale) then
                               concat('round("',column_name,'",',V_NUMERIC_SCALE,')')
                            else 
                              concat('"',column_name,'"') 
                          end 
                        else concat('"',column_name,'"') 
                      end 
                      order by ordinal_position separator ',')  "TARGET_COLUMNS"
   from (
     select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,column_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision,cc.check_clause 
       from information_schema.columns c
       left join information_schema.tables t
         on t.table_name = c.table_name 
        and t.table_schema = c.table_schema
        left outer join information_schema.check_constraints cc
          on cc.table_name = c.table_name 
         and cc.constraint_schema = c.table_schema
		 and cc.constraint_name = c.column_name
         and check_clause = concat('json_valid("',column_name,'")')  
       where c.extra <> 'VIRTUAL GENERATED'
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
    insert into SCHEMA_COMPARE_RESULTS values (P_SOURCE_SCHEMA,P_TARGET_SCHEMA,V_TABLE_NAME,-1,-1,-1,-1,concat(V_SQLERRM,V_STATEMENT));
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
    set MISSING_TABLE = false;
    FETCH TABLE_METADATA INTO V_TABLE_NAME, V_SOURCE_COLUMN_LIST, V_TARGET_COLUMN_LIST;
    IF NO_MORE_ROWS THEN
      LEAVE PROCESS_TABLE;
    END IF;

    TRUNCATE TABLE SOURCE_HASH_TABLE;
    TRUNCATE TABLE TARGET_HASH_TABLE;
	
	set V_STATEMENT = CONCAT('insert into SOURCE_HASH_TABLE select UNHEX(SHA2(JSON_ARRAY(',V_SOURCE_COLUMN_LIST,'),256)) HASH from "',P_SOURCE_SCHEMA,'"."',V_TABLE_NAME,'"');
    set @STATEMENT = V_STATEMENT;
    PREPARE STATEMENT FROM @STATEMENT;
    EXECUTE STATEMENT;
    DEALLOCATE PREPARE STATEMENT;
    
	set V_STATEMENT = CONCAT('insert into TARGET_HASH_TABLE select UNHEX(SHA2(JSON_ARRAY(',V_TARGET_COLUMN_LIST,'),256)) HASH from "',P_TARGET_SCHEMA,'"."',V_TABLE_NAME,'"');
    set @STATEMENT = V_STATEMENT;
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
