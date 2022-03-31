SET SESSION SQL_MODE=ANSI_QUOTES;
--
DROP PROCEDURE IF EXISTS GENERATE_SQL;
--
DELIMITER $$
--
CREATE PROCEDURE GENERATE_SQL(P_VENDOR VARCHAR(128), P_TARGET_SCHEMA VARCHAR(128), P_TABLE_NAME VARCHAR(128), P_COLUMN_NAME_ARRAY JSON, P_DATA_TYPE_ARRAY JSON, P_SIZE_CONSTRAINT_ARRAY JSON, P_SPATIAL_FORMAT VARCHAR(7), P_CIRCLE_FORMAT VARCHAR(7), OUT P_TABLE_INFO JSON)
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

  -- select P_VENDOR, P_TARGET_SCHEMA, P_TABLE_NAME, P_COLUMN_NAME_ARRAY, P_DATA_TYPE_ARRAY, P_SIZE_CONSTRAINT_ARRAY, P_SPATIAL_FORMAT, P_CIRCLE_FORMAT;
  
  with 
    "SOURCE_TABLE_DEFINITIONS" 
    as ( 
      select c."KEY" IDX
            ,c.VALUE "COLUMN_NAME"
            ,t.VALUE "DATA_TYPE"
			,case 
			   when (P_VENDOR = 'MySQL') then 
			     t.VALUE
               else
			     m.MYSQL_TYPE
			  end "MYSQL_TYPE"
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
           join JSON_TABLE(P_DATA_TYPE_ARRAY,       '$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(128) PATH '$')) t on (c."KEY" = t."KEY") 
           join JSON_TABLE(P_SIZE_CONSTRAINT_ARRAY, '$[*]' COLUMNS ("KEY" FOR ORDINALITY, "VALUE" VARCHAR(32)  PATH '$')) s on (c."KEY" = s."KEY")
		   left outer join TYPE_MAPPINGS m on (lower(t."VALUE") = lower(m."VENDOR_TYPE"))
    ),
    "TARGET_TABLE_DEFINITIONS" 
    as (
      select st.*
             -- AVOID ERROR 1271 (HY000) at line 37: Illegal mix of collations for operation 'concat'
            ,CAST(MAP_MYSQL_DATATYPE(P_VENDOR,"MYSQL_TYPE","DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE",P_CIRCLE_FORMAT) as CHAR) TARGET_DATA_TYPE
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
                                 -- when TARGET_DATA_TYPE in ('nchar','nvarchar')  then
                                   -- then concat('(',DATA_TYPE_LENGTH,')',' CHARACTER SET UTF8MB4 ')
                                 when DATA_TYPE_SCALE is not NULL then
                                   concat(TARGET_DATA_TYPE,'(',CAST(DATA_TYPE_LENGTH as CHAR),',',CAST(case when (DATA_TYPE_SCALE > 30) then 30 else DATA_TYPE_SCALE end as CHAR),')')
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

  -- select  V_COLUMN_LIST, V_COLUMNS_CLAUSE, V_TARGET_DATA_TYPES, V_INSERT_SELECT_LIST, V_COLUMN_PATTERNS;

  SET V_DDL_STATEMENT = concat('create table if not exists "',P_TARGET_SCHEMA,'"."',P_TABLE_NAME,'"(',V_COLUMNS_CLAUSE,')'); 
  SET V_DML_STATEMENT = concat('insert into "',P_TARGET_SCHEMA,'"."',P_TABLE_NAME,'"(',V_COLUMN_LIST,')',CHAR(32),'select ',V_INSERT_SELECT_LIST,CHAR(32),'  from "YADAMU_STAGING" js,JSON_TABLE(js."DATA",''$.data."',P_TABLE_NAME,'"[*]'' COLUMNS (',CHAR(32),V_COLUMN_PATTERNS,')) data');     
  SET P_TABLE_INFO = JSON_OBJECT('ddl',V_DDL_STATEMENT,'dml',V_DML_STATEMENT,'targetDataTypes',CAST(V_TARGET_DATA_TYPES as JSON));
    
END;
$$
--
DELIMITER ;
--  