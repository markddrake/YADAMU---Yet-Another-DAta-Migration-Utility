/*
**
** Drop functions regardless of signature
**
*/
do 
$$
declare
   _count numeric;
   _sql text;
begin
   SELECT count(*)::int
        , 'DROP FUNCTION ' || string_agg(oid::regprocedure::text, '; DROP FUNCTION ')
   FROM   pg_proc
   WHERE  UPPER(proname) in ('YADAMU_EXPORT','MAP_FOREIGN_DATA_TYPE','MAP_PGSQL_DATA_TYPE','GENERATE_STATEMENTS','YADAMU_IMPORT_JSON','YADAMU_IMPORT_JSONB','GENERATE_SQL','EXPORT_JSON','IMPORT_JSON','IMPORT_JSONB')
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed

   if _count > 0 then
     execute _sql;
   end if;
   SELECT count(*)::int
        , 'DROP PROCEDURE ' || string_agg(oid::regprocedure::text, '; DROP PROCEDURE ')
   FROM   pg_proc
   WHERE  UPPER(proname) in ('SET_VENDOR_TYPE_MAPPINGS')
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed

   if _count > 0 then
     execute _sql;
   end if;
end
$$ 
language plpgsql;
--
/*
**
** Yadamu Instance ID and Installation Timestamp
**
*/
do $$ 
declare
   COMMAND              character varying (256);
   V_YADAMU_INSTANCE_ID character varying(36);
begin

  begin
    SELECT YADAMU_INSTANCE_ID() into V_YADAMU_INSTANCE_ID;
  exception
    when undefined_function then
      V_YADAMU_INSTANCE_ID := upper(gen_random_uuid()::CHAR(36));
    when others then
      RAISE;
   end;
   
   COMMAND  := concat('CREATE OR REPLACE FUNCTION YADAMU_INSTANCE_ID() RETURNS CHARACTER VARYING STABLE AS $X$ select ''',V_YADAMU_INSTANCE_ID,''' $X$ LANGUAGE SQL');
   EXECUTE COMMAND;
end $$;
--
do $$ 
declare
   COMMAND                   character varying(256);
   V_INSTALLATION_TIMESTAMP  character varying(27);
begin
  V_INSTALLATION_TIMESTAMP := to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM');
  COMMAND  := concat('CREATE OR REPLACE FUNCTION YADAMU_INSTALLATION_TIMESTAMP() RETURNS CHARACTER VARYING STABLE AS $X$ select ''',V_INSTALLATION_TIMESTAMP,''' $X$ LANGUAGE SQL');
  EXECUTE COMMAND;
end $$;
--
CREATE OR REPLACE FUNCTION YADAMU_AsPointArray(polygon)
returns point[]
STABLE RETURNS NULL ON NULL INPUT
as
$$
--
-- Array of Points from Polygon
--
select array_agg(POINT(V))
  from unnest(string_to_array(ltrim(rtrim($1::VARCHAR,'))'),'(('),'),(')) V
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU_AsPointArray(path)
returns point[]
STABLE RETURNS NULL ON NULL INPUT
as
$$
--
-- Array of Points from Path
--
select array_agg(POINT(V))
  from unnest(string_to_array(ltrim(rtrim($1::VARCHAR,')])'),'[('),'),(')) V
$$
LANGUAGE SQL;
--
/*
**
** JSON Based Export/Import of TSVECTOR
**
*/
--
CREATE OR REPLACE FUNCTION YADAMU_AsJSON(tsvector) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select array_to_json(tsvector_to_array($1))::jsonb
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU_AsTSVector(jsonb) 
returns tsvector
STABLE RETURNS NULL ON NULL INPUT
as
$$
select array_to_tsvector(array_agg(value)) from jsonb_array_elements_text($1)
$$
LANGUAGE SQL;
--
/*
**
** JSON Based Export/Import of RANGE types
**
*/
CREATE OR REPLACE FUNCTION YADAMU_AsJSON(anyRange) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
  'hasLowerBound',
  not lower_inf($1),
  'lowerBound',
  lower($1),
  'includeLowerBound',
  lower_inc($1),
  'hasUpperBound',
  not upper_inf($1),
  'upperBound',  
  upper($1),
  'includeUpperBound',
  upper_inc($1)
)
$$
LANGUAGE SQL;
--
create or replace function YADAMU_AsRange(jsonb)
returns character varying
STABLE RETURNS NULL ON NULL INPUT
as
$$
select case when ($1 ->> 'includeLowerBound')::boolean then '[' else '(' end
	|| case when ($1 ->> 'hasLowerBound')::boolean  then '"' || ($1 ->> 'lowerBound') || '"' else '' end
	|| ','
	|| case when ($1 ->> 'hasUpperBound')::boolean then '"' || ($1 ->> 'upperBound') || '"' else '' end
    || case when ($1 ->> 'includeUpperBound')::boolean then ']' else ')' end;
$$
LANGUAGE SQL;
--
/*
**
** GeoJSON Based Export/Import of LINE 
**
*/
--
CREATE OR REPLACE FUNCTION YADAMU_AsGEOJSON(line) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'Feature',
         'geometry',
         jsonb_build_object(
           'type',
           'Point',
           'coordinates',
           jsonb_build_array(
             ($1)[0],
             ($1)[1]
           )
         ),
        'shape',
        'LinearEquation',
        'properties',
        jsonb_build_object(
          'constant',
           $1[2]
        )
      )
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU_AsLine(jsonb) 
returns line
STABLE RETURNS NULL ON NULL INPUT
as
$$
select ('{' || ($1 #>> '{geometry,coordinates,0}') || ',' || ($1 #>> '{geometry,coordinates,1}') || ',' || ($1 #>> '{properties,constant}') || '}')::line
$$
LANGUAGE SQL;
--
/*
**
** GeoJSON Based Export/Import of CIRCLE 
**
*/
--
CREATE OR REPLACE FUNCTION YADAMU_AsGEOJSON(circle) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'Feature',
         'geometry',
         jsonb_build_object(
           'type',
           'Point',
           'coordinates',
           jsonb_build_array(
             (center($1))[0],
             (center($1))[1]
           )
         ),
         'shape',
         'Circle',
         'properties',
         jsonb_build_object(
         'radius',
            radius($1)
         )
       )
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU_AsCircle(jsonb) 
returns circle
STABLE RETURNS NULL ON NULL INPUT
as
$$
select ('<(' || ($1 #>> '{geometry,coordinates,0}') || ',' || ($1 #>> '{geometry,coordinates,1}') || '),' || ($1 #>> '{properties,radius}') || '>')::circle
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU_AsLSeg(path) 
returns LSeg
STABLE RETURNS NULL ON NULL INPUT
as
--
-- PATH to LSEG
--
$$
with POINTS as (
  select YADAMU_asPointArray($1) P
)   
select lseg(point(p[1]),point(p[2]))
  from POINTS;
$$
LANGUAGE SQL;
--
/*
**
** Postgress YADAMU_EXPORT Function.
**
*/
--
create or replace function YADAMU_EXPORT(P_SCHEMA VARCHAR, P_SPATIAL_FORMAT VARCHAR, P_OPTIONS JSONB DEFAULT '{"circleAsPolygon": false}'::JSONB) 
returns TABLE ( TABLE_SCHEMA VARCHAR, TABLE_NAME VARCHAR, COLUMN_NAME_ARRAY JSONB, DATA_TYPE_ARRAY JSONB, SIZE_CONSTRAINT_ARRAY JSONB, CLIENT_SELECT_LIST TEXT, ERRORS JSONB)
as $$
declare
  R                       RECORD;
  V_SQL_STATEMENT         TEXT = NULL;
  PLPGSQL_CTX             TEXT;
  C_CHARACTER_MAX_LENGTH  character varying (20) = cast(1*1024*1024*1024 as character varying(20));
  V_SIZE_CONSTRAINTS      JSONB;
begin

  for r in select t.table_schema "TABLE_SCHEMA"
                 ,t.table_name "TABLE_NAME"
	             ,jsonb_agg(column_name order by ordinal_position) "COLUMN_NAME_ARRAY"
	             ,jsonb_agg(case 
				              when ((c.data_type = 'character') and (c.character_maximum_length is null)) then
                                c.udt_name 
                              when c.data_type = 'USER-DEFINED' then
                                c.udt_name 
                              when c.data_type = 'ARRAY' then
                                e.data_type || ' ARRAY'
                              when ((c.data_type = 'interval') and (c.interval_type is not null)) then
							    c.data_type || ' ' || c.interval_type
                              else 
                                c.data_type 
                            end 
                            order by ordinal_position
                           ) "DATA_TYPE_ARRAY"
                 ,jsonb_agg(case
                              when (c.numeric_precision is not null) and (c.numeric_scale is not null) then
                                cast(c.numeric_precision as varchar) || ',' || cast(c.numeric_scale as varchar)
                              when (c.numeric_precision is not null) then 
                                cast(c.numeric_precision as varchar)
                              when (c.character_maximum_length is not null) then 
                                cast(c.character_maximum_length as varchar)
                              when (c.datetime_precision is not null) then 
                                cast(c.datetime_precision as varchar)
							  when ((c.data_type in ('character','character varying','text')) and (c.character_maximum_length is null)) then
							    C_CHARACTER_MAX_LENGTH
                            end
                            order by ordinal_position
                          ) "SIZE_CONSTRAINT_ARRAY"
	             ,string_agg(case 
                               when c.data_type = 'xml' then
                                  '"' || column_name || '"' || '::text'
                               when c.data_type in ('money') then
                                 /* Suppress printing Currency Symbols */
                                 /* then '("' || column_name || '"/1::money) */
                                 '("' || column_name || '"::numeric) "' || COLUMN_NAME || '"'
                               when c.data_type in ('date','timestamp without time zone') then
                                 'to_char ("' || COLUMN_NAME ||'" at time zone ''UTC'', ''YYYY-MM-DD"T"HH24:MI:SS.US"Z"'') "' || COLUMN_NAME || '"'
                               when c.data_type in ('timestamp with time zone') then
                                 'to_char ("' || COLUMN_NAME ||'", ''YYYY-MM-DD"T"HH24:MI:SS.US"Z"'') "' || COLUMN_NAME || '"'
                               when c.data_type in ('time without time zone') then
                                 'to_char ((''epoch''::date + "' || COLUMN_NAME || '")::timestamp at time zone ''UTC'', ''YYYY-MM-DD"T"HH24:MI:SS.US'') "' || COLUMN_NAME || '"'
                               when c.data_type in ('time with time zone') then
							     'to_char ((''epoch''::date + "' || COLUMN_NAME || '")::timestamptz, ''YYYY-MM-DD"T"HH24:MI:SS.US'') "' || COLUMN_NAME || '"'
							   when c.data_type like 'interval%' then
                                 '"' || COLUMN_NAME ||'"::varchar "' || COLUMN_NAME || '"'
                               when ((c.data_type = 'USER-DEFINED') and (c.udt_name in ('geometry')))  then
                                 case 
                                   when P_SPATIAL_FORMAT = 'WKB' then
                                     'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'WKT' then
                                     'ST_AsText("' || column_name || '",18) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKB' then
                                     'ST_AsEWKB("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKT' then
                                     'ST_AsEWKT("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'GeoJSON' then
                                     'ST_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                   else
								     '"' || column_name || '"::VARCHAR "' || COLUMN_NAME || '"'
                                 end
                               when ((c.data_type = 'USER-DEFINED') and (c.udt_name in ('geography')))  then
                                 case 
                                   when P_SPATIAL_FORMAT = 'WKB' then
                                     'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'WKT' then
                                     'ST_AsText("' || column_name || '",18) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKB' then
                                     'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKT' then
                                     'ST_AsText("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'GeoJSON' then
                                     'ST_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                   else
								     '"' || column_name || '"::VARCHAR "' || COLUMN_NAME || '"'
                                 end
                               when (c.data_type in ('point','path','polygon')) then
                                 case 
                                   when P_SPATIAL_FORMAT = 'WKB' then
                                     'ST_AsBinary("' || column_name || '"::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'WKT' then
                                     'ST_AsText("' || column_name || '"::geometry,18) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKB' then
                                     'ST_AsBinary("' || column_name || '"::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKT' then
                                     'ST_AsEWKT("' || column_name || '"::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'GeoJSON' then
                                     'ST_AsText("' || column_name || '"::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'YadamuJSON' then
								     'YADAMU_AsGeoJSON("' || column_name || '" ) "' || COLUMN_NAME || '"'
                                   else
								     '"' || column_name || '"::VARCHAR "' || COLUMN_NAME || '"'
                                 end
                               when (c.data_type = 'line') then
                                 'YADAMU_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
 							   when (c.data_type = 'lseg') then
                                 case 
                                   when P_SPATIAL_FORMAT = 'WKB' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsBinary(path(concat(''('',("' || column_name || '")[0]::VARCHAR,'','',("' || column_name || '")[1],'')''))::geometry) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'WKT' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsText((path(concat(''('',("' || column_name || '")[0]::VARCHAR,'','',("' || column_name || '")[1],'')''))::geometry,18) end "' || COLUMN_NAME || '"' 
                                   when P_SPATIAL_FORMAT = 'EWKB' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsEWKB((path(concat(''('',("' || column_name || '")[0]::VARCHAR,'','',("' || column_name || '")[1],'')''))::geometry)) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKT' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsEWKT((path(concat(''('',("' || column_name || '")[0]::VARCHAR,'','',("' || column_name || '")[1],'')''))::geometry) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'GeoJSON' then
                                      'case when "' || column_name || '" is NULL then NULL else ST_AsGeoJSON((path(concat(''('',("' || column_name || '")[0]::VARCHAR,'','',("' || column_name || '")[1],'')''))::geometry) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'YadamuJSON' then
								     'YADAMU_AsGeoJSON("' || column_name || '" ) "' || COLUMN_NAME || '"'
                                   else
								     '"' || column_name || '"::VARCHAR "' || COLUMN_NAME || '"'
                                 end
                               when (c.data_type = 'box') then
                                 case 
								   when P_SPATIAL_FORMAT = 'WKB' then
                                     'ST_AsBinary(polygon("' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'WKT' then
                                     'ST_AsText(polygon("' || column_name || '")::geometry,18) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKB' then
                                      'ST_AsEWKB(polygon("' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKT' then
                                     'ST_AsEWKT(polygon("' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'GeoJSON' then
                                     'ST_AsGeoJSON(polygon("' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'YadamuJSON' then
								     'YADAMU_AsGeoJSON("' || column_name || '" ) "' || COLUMN_NAME || '"'
                                   else
								     '"' || column_name || '" ::VARCHAR "' || COLUMN_NAME || '"'
                                 end
                               when (c.data_type = 'circle') then
                                 case 
								   when ((P_OPTIONS ->> 'circleAsPolygon')::boolean = true) then
								     case 
                                       when P_SPATIAL_FORMAT = 'WKB' then
                                         'ST_AsBinary(polygon(32,"' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                       when P_SPATIAL_FORMAT = 'WKT' then
                                         'ST_AsText(polygon(32,"' || column_name || '")::geometry,18) "' || COLUMN_NAME || '"'
                                       when P_SPATIAL_FORMAT = 'EWKB' then
                                         'ST_AsEWKB(polygon(32,"' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                       when P_SPATIAL_FORMAT = 'EWKT' then
                                         'ST_AsEWKT(polygon(32,"' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                       when P_SPATIAL_FORMAT = 'GeoJSON' then
                                         'ST_AsGeoJSON(polygon(32,"' || column_name || '")::geometry) "' || COLUMN_NAME || '"'
                                       when P_SPATIAL_FORMAT = 'YadamuJSON' then
								        'YADAMU_AsGeoJSON(polygon(32,"' || column_name || '")) "' || COLUMN_NAME || '"'
                                       else
								        '"' || column_name || '"::VARCHAR "' || COLUMN_NAME || '"'
								     end
								   else 
                                     'YADAMU_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
							     end  
                               when (c.data_type in ('int4range','int8range','numrange','tsrange','tstzrange','daterange','tsvector')) then
                                 'YADAMU_AsJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                               else
                                 '"' || column_name || '"'
                             end,
			                 ',' order by ordinal_position
                               ) "CLIENT_SELECT_LIST"
            from information_schema.columns c
  			left join information_schema.tables t 
			  on ((t.table_schema, t.table_name) = (c.table_schema, c.table_name))
	        left join information_schema.element_types e
              on ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier) = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
           where t.table_type = 'BASE TABLE'
             and t.table_schema =  P_SCHEMA
        group by t.table_schema, t.table_name
    
  loop

    /*
    **
    ** Calculate the max length of bytea fields, since Postgres does not maintain a maximum length in the dictionary.
    **
    */
    select 'select jsonb_build_array(' 
        || string_agg(
             case
               when value = 'bytea' 
                 then 'to_char(max(octet_length("' || (r."COLUMN_NAME_ARRAY" ->> cast((idx -1) as int)) || '")),''FM999999999999999999'')'
               else
                 '''' || COALESCE ((r."SIZE_CONSTRAINT_ARRAY" ->> cast((idx -1) as int)),'') || ''''
             end
            ,','
           ) 
        || ') FROM "' || r."TABLE_SCHEMA" || '"."' || r."TABLE_NAME" || '"'
      from  jsonb_array_elements_text(r."DATA_TYPE_ARRAY") WITH ORDINALITY as c(value, idx)
      into V_SQL_STATEMENT
     where r."DATA_TYPE_ARRAY" ? 'bytea';

    if (V_SQL_STATEMENT is not NULL) then
      execute V_SQL_STATEMENT into V_SIZE_CONSTRAINTS;
    else
      V_SIZE_CONSTRAINTS := r."SIZE_CONSTRAINT_ARRAY";
    end if;

    TABLE_SCHEMA          := r."TABLE_SCHEMA";
    TABLE_NAME            := r."TABLE_NAME";
    COLUMN_NAME_ARRAY     := r."COLUMN_NAME_ARRAY";
    DATA_TYPE_ARRAY       := r."DATA_TYPE_ARRAY";
    SIZE_CONSTRAINT_ARRAY := V_SIZE_CONSTRAINTS;
    CLIENT_SELECT_LIST    := r."CLIENT_SELECT_LIST";
    return NEXT;
  end loop;
  return;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    ERRORS := '[]';
    ERRORS := jsonb_insert(ERRORS, CAST('{' || jsonb_array_length(ERRORS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','call YADAMU_EXPORT(P_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    return NEXT;
    return;
end;  
$$ LANGUAGE plpgsql;
--
/*
**
** Postgress YADAMU_IMPORT_JSON Function.
**
*/
--
create or replace procedure SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS JSONB) 
as $$
begin

  create temporary table if not exists TYPE_MAPPING (
    "VENDOR_TYPE"   VARCHAR(256)
   ,"PGSQL_TYPE"    VARCHAR(256)
  );
  
  delete from TYPE_MAPPING;
  
  insert into TYPE_MAPPING
  select VALUE ->> 0 "VENDOR_TYPE", value ->> 1 "PGSQL_TYPE" 
    from jsonb_array_elements (P_TYPE_MAPPINGS);
		 
end;  
$$ LANGUAGE plpgsql;
--
create or replace function MAP_PGSQL_DATA_TYPE(P_VENDOR VARCHAR, P_PGSQL_DATA_TYPE  VARCHAR, P_DATA_TYPE VARCHAR, P_DATA_TYPE_LENGTH BIGINT, P_DATA_TYPE_SCALE INT, P_JSON_DATA_TYPE VARCHAR, P_POSTGIS_INSTALLED BOOLEAN) 
returns VARCHAR
as $$
declare
  C_CLOB_TYPE                         VARCHAR(32) = 'text';
  C_ORACLE_OBJECT_TYPE                VARCHAR(32) = C_CLOB_TYPE;

  /*
  C_CHAR_TYPE                         VARCHAR(32) = 'character';
  
  C_GEOMETRY_TYPE                     VARCHAR(32) = CASE WHEN P_POSTGIS_INSTALLED THEN 'geometry' ELSE 'JSON' END;
  C_GEOGRAPHY_TYPE                    VARCHAR(32) = CASE WHEN P_POSTGIS_INSTALLED THEN 'geography' ELSE 'JSON' END;
  C_MAX_CHARACTER_VARYING_TYPE_LENGTH INT         = 10 * 1024 * 1024;
  C_MAX_CHARACTER_VARYING_TYPE        VARCHAR(32) = 'character varying(' || C_MAX_CHARACTER_VARYING_TYPE_LENGTH || ')';
  C_BFILE_TYPE                        VARCHAR(32) = 'character varying(2048)';
  C_ROWID_TYPE                        VARCHAR(32) = 'character varying(18)';
  C_MYSQL_TINY_TEXT_TYPE              VARCHAR(32) = 'character varying(256)';
  C_MYSQL_TEXT_TYPE                   VARCHAR(32) = 'character varying(65536)';
  C_ENUM_TYPE                         VARCHAR(32) = 'character varying(255)';
  C_MSSQL_MONEY_TYPE                  VARCHAR(32) = 'numeric(19,4)';
  C_MSSQL_SMALL_MONEY_TYPE            VARCHAR(32) = 'numeric(10,4)';
  C_HIERARCHY_TYPE                    VARCHAR(32) = 'character varying(4000)';
  C_INET_ADDR_TYPE                    VARCHAR(32) = 'character varying(39)';
  C_MAC_ADDR_TYPE                     VARCHAR(32) = 'character varying(23)';
  C_UNSIGNED_INT_TYPE                 VARCHAR(32) = 'numeric(10,0)';
  C_PGSQL_IDENTIFIER                  VARCHAR(32) = 'binary(4)';
  C_MONGO_OBJECT_ID                   VARCHAR(32) = 'binary(12)';
  C_MONGO_UNKNOWN_TYPE                VARCHAR(32) = 'character varying(2048)';
  C_MONGO_REGEX_TYPE                  VARCHAR(32) = 'character varying(2048)';

  V_DATA_TYPE                         VARCHAR(128);
  */
  
begin
  case 
    when ((P_PGSQL_DATA_TYPE is null) and (P_VENDOR = 'Oracle') and (P_DATA_TYPE like '"%"."%"')) then
	  return C_ORACLE_OBJECT_TYPE;
    when P_PGSQL_DATA_TYPE is null then
	
      raise exception 'Postgres: Missing mapping for "%" datatype "%"', P_VENDOR, P_DATA_TYPE;
	else
	  return P_PGSQL_DATA_TYPE;
  end case;
end;  
$$ LANGUAGE plpgsql;
--
create or replace function GENERATE_SQL(P_VENDOR VARCHAR,P_TARGET_SCHEMA VARCHAR, P_TABLE_NAME VARCHAR, P_COLUMN_NAME_ARRAY JSONB, P_DATA_TYPE_ARRAY JSONB, P_SIZE_CONSTRAINT_ARRAY JSONB, P_SPATIAL_FORMAT VARCHAR, P_JSON_DATA_TYPE VARCHAR, P_BINARY_JSON BOOLEAN)
returns JSONB
as $$
declare
  V_COLUMN_LIST        TEXT;
  V_COLUMNS_CLAUSE     TEXT;
  V_INSERT_SELECT_LIST TEXT;
  V_TARGET_DATA_TYPES  JSONB;
  V_POSTGIS_VERSION    VARCHAR(512);
  V_POSTGIS_ENABLED    BOOLEAN = FALSE;
  C_MAX_BOUNDED_LENGTH INT = 10 * 1024 * 1024;
begin

  begin
    SELECT PostGIS_full_version() into V_POSTGIS_VERSION;
    V_POSTGIS_ENABLED = TRUE;
  exception
    when undefined_function then
      V_POSTGIS_ENABLED = FALSE;
    when others then
      RAISE;
  end;

  with
  SOURCE_TABLE_DEFINITIONS
  as (
    select c.IDX
          ,c.VALUE COLUMN_NAME
          ,t.VALUE DATA_TYPE
		  ,case 
		     when P_VENDOR = 'Postgres'  then
			   t.VALUE 
			 else 
			   m."PGSQL_TYPE"
		   end "PGSQL_TYPE"
          ,case
             when s.VALUE = ''
               then NULL
             when strpos(s.VALUE,',') > 0
               then SUBSTR(s.VALUE,1,strpos(s.VALUE,',')-1)
             else
               s.VALUE
            end DATA_TYPE_LENGTH
          ,case
             when strpos(s.VALUE,',') > 0
               then SUBSTR(s.VALUE, strpos(s.VALUE,',')+1)
              else
                NULL
           end DATA_TYPE_SCALE
      from JSONB_ARRAY_ELEMENTS_TEXT(P_COLUMN_NAME_ARRAY)     WITH ORDINALITY as c(VALUE, IDX)
      join JSONB_ARRAY_ELEMENTS_TEXT(P_DATA_TYPE_ARRAY)       WITH ORDINALITY as t(VALUE, IDX) on c.IDX = t.IDX
      join JSONB_ARRAY_ELEMENTS_TEXT(P_SIZE_CONSTRAINT_ARRAY) WITH ORDINALITY as s(VALUE, IDX) on c.IDX = s.IDX
      left outer join TYPE_MAPPING m on lower(t.VALUE) = lower(m."VENDOR_TYPE")
	  -- left outer join TYPE_MAPPING m on t.VALUE = m."VENDOR_TYPE"
  ),
  TARGET_TABLE_DEFINITIONS
  as (
    select st.*,
           MAP_PGSQL_DATA_TYPE(P_VENDOR,"PGSQL_TYPE",DATA_TYPE,DATA_TYPE_LENGTH::BIGINT,DATA_TYPE_SCALE::INT, P_JSON_DATA_TYPE, V_POSTGIS_ENABLED) TARGET_DATA_TYPE
      from SOURCE_TABLE_DEFINITIONS st
  ) 
  select STRING_AGG('"' || COLUMN_NAME || '"',',' ORDER BY IDX) COLUMN_NAME_LIST,
         STRING_AGG('"' || COLUMN_NAME || '" ' || TARGET_DATA_TYPE || 
                    case 
                      when TARGET_DATA_TYPE like '%(%)' then 
                        ''
                      when TARGET_DATA_TYPE like '%with time zone' then 
                        ''
                      when TARGET_DATA_TYPE like '%without time zone' then 
                        ''
                      when TARGET_DATA_TYPE in ('boolean','smallint', 'mediumint', 'int', 'bigint','real','text','bytea','integer','money','xml','json','jsonb','image','date','double precision','geography','geometry') then 
                        ''
                      when ((TARGET_DATA_TYPE in ('character','character varying','text','bpchar')) and (cast(DATA_TYPE_LENGTH as INT) > C_MAX_BOUNDED_LENGTH)) then 
                        ''
                      when (TARGET_DATA_TYPE = 'time' and DATA_TYPE_LENGTH::INT > 6) then 
                        '(6)'
                      when TARGET_DATA_TYPE like 'interval%' then 
                        ''
				      when DATA_TYPE_LENGTH::numeric < 1 then
					    ''
                      when DATA_TYPE_LENGTH is NOT NULL and DATA_TYPE_SCALE IS NOT NULL then 
                        '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
                      when DATA_TYPE_LENGTH is NOT NULL then 
                        '(' || DATA_TYPE_LENGTH  || ')'
                      else
                        ''
                    end
                   ,CHR(10) || '  ,' ORDER BY IDX
                   ) COLUMNS_CLAUSE
        ,STRING_AGG(case 
                      when TARGET_DATA_TYPE = 'bytea' then  
                        'decode( value ->> ' || IDX-1 || ',''hex'')'
                      when TARGET_DATA_TYPE in ('geometry','geography') then 
                        case 
                          when P_SPATIAL_FORMAT = 'WKB' then
                            'ST_GeomFromWKB(decode( value ->> ' || IDX-1 || ',''hex''))'
                          when P_SPATIAL_FORMAT = 'EWKB' then
                            'ST_GeomFromEWKB(decode( value ->> ' || IDX-1 || ',''hex''))'
                          when P_SPATIAL_FORMAT = 'WKT' then
                            'ST_GeomFromText( value ->> ' || IDX-1 || ')'
                          when P_SPATIAL_FORMAT = 'EWKT' then
                            'ST_GeomFromEWKT( value ->> ' || IDX-1 || ')'
                          when P_SPATIAL_FORMAT = 'GeoJSON' then
                            'ST_GeomFromGeoJSON( value ->> ' || IDX-1 || ')'
                          end
                      when TARGET_DATA_TYPE in ('time', 'time with time zone','time without time zone')  then  
                        'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE like 'time%time zone'  then  
                        'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE in ('time', 'time with time zone','time without time zone')  then  
                        'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE = 'bit' then  
                        'case when value ->> ' || IDX-1 || ' = ''true'' then B''1'' when value ->> ' || IDX-1 || ' = ''false''  then B''0'' else cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ') end'
                      else
                       'cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ')'
                    end 
                    || ' "' || COLUMN_NAME || '"', CHR(10) || '  ,' ORDER BY IDX
                   ) INSERT_SELECT_LIST
        ,JSONB_AGG(TARGET_DATA_TYPE ORDER BY IDX) TARGET_DATA_TYPES
    into V_COLUMN_LIST, V_COLUMNS_CLAUSE, V_INSERT_SELECT_LIST, V_TARGET_DATA_TYPES
    from TARGET_TABLE_DEFINITIONS;

  return JSONB_BUILD_OBJECT(
                    'ddl', 'CREATE TABLE IF NOT EXISTS "' || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '"(' || CHR(10) || '   ' || V_COLUMNS_CLAUSE || CHR(10) || ')'
                    ,'dml', 'INSERT into "' || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '"(' || V_COLUMN_LIST || ')' || CHR(10) || 'select ' || V_INSERT_SELECT_LIST || CHR(10) || '  from ' || case WHEN P_BINARY_JSON then 'jsonb_array_elements' else 'json_array_elements' end || '($1 -> ''data'' -> ''' || P_TABLE_NAME || ''')'
                    ,'targetDataTypes', V_TARGET_DATA_TYPES
               );
end;  
$$ LANGUAGE plpgsql;
--
create or replace function YADAMU_IMPORT_JSONB(P_JSON jsonb, P_TYPE_MAPPINGS jsonb, P_TARGET_SCHEMA VARCHAR, P_OPTIONS jsonb) 
returns JSONB
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB = '[]';
  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     NUMERIC;

  PLPGSQL_CTX        TEXT;
begin

  call SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS);

  for r in select "tableName"
                 ,GENERATE_SQL(P_JSON #>> '{systemInformation,vendor}',P_TARGET_SCHEMA,"tableName", "columnNames", "dataTypes", "sizeConstraints", P_JSON #>> '{systemInformatio,typeMappings,spatialFormat}', P_OPTIONS ->> 'jsonStorageOption', TRUE) "TABLE_INFO"
             from JSONB_EACH(P_JSON -> 'metadata')  
                  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA(
                                                                "tableSchema"      VARCHAR, 
                                                                "tableName"        VARCHAR, 
                                                                "columnsNames"     JSONB, 
                                                                "dataTypes"        JSONB, 
                                                                "sizeConstraints"  JSONB
                                                              ) 
  loop
                                                              
    begin
      EXECUTE r."TABLE_INFO" ->> 'ddl';
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('ddl', jsonb_build_object('tableName',r."tableName",'sqlStatement',r."TABLE_INFO"->>'ddl')), TRUE);
    exception 
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO"->>'ddl','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end; 
  
    begin
      V_START_TIME := clock_timestamp();
      EXECUTE r."TABLE_INFO" ->> 'dml'  using  P_JSON;
      V_END_TIME := clock_timestamp();
      GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
      V_ELAPSED_TIME :=  round(((extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME)) * 1000)::NUMERIC,4);
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS)  || '}' as TEXT[]), jsonb_build_object('dml', jsonb_build_object('tableName',r."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME,'sqlStatement',r."TABLE_INFO"->>'dml')), true);
    exception
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO"->>'dml','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end;

  end loop;
  return V_RESULTS;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','call YADAMU_IMPORT_JSONB(P_JSON jsonb,P_TARGET_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    return V_RESULTS;
end;  
$$ LANGUAGE plpgsql;
--
create or replace function YADAMU_IMPORT_JSON(P_JSON json, P_TYPE_MAPPINGS jsonb, P_TARGET_SCHEMA VARCHAR, P_OPTIONS jsonb) 
returns JSONB
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB = '[]';
  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     NUMERIC;

  PLPGSQL_CTX        TEXT;
begin

  call SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS);

  for r in select "tableName"
                 ,GENERATE_SQL(P_JSON #>> '{systemInformation,vendor}',P_TARGET_SCHEMA,"tableName","columnNames","dataTypes","sizeConstraints", P_JSON #>> '{systemInformation,typeMappings,spatialFormat}', P_OPTIONS ->> 'jsonStorageOption', FALSE) "TABLE_INFO"
             from JSON_EACH(P_JSON -> 'metadata')  
                  CROSS JOIN LATERAL JSON_TO_RECORD(value) as METADATA(
                                                               "tableSchema"      VARCHAR, 
                                                               "tableName"        VARCHAR, 
                                                               "columnNames"      JSONB,
                                                               "dataTypes"        JSONB, 
                                                               "sizeConstraints"  JSONB
                                                              ) 
  loop
  
    begin
      EXECUTE r."TABLE_INFO" ->> 'ddl';
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('ddl', jsonb_build_object('tableName',r."tableName",'sqlStatement',r."TABLE_INFO" ->> 'ddl')), true);
    exception 
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO" ->> 'ddl','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end; 
  
    begin
      V_START_TIME := clock_timestamp();
      EXECUTE r."TABLE_INFO" ->> 'dml'  using  P_JSON;
      V_END_TIME := clock_timestamp();
      GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
      V_ELAPSED_TIME :=  round(((extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME)) * 1000)::NUMERIC,4);
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS)  || '}' as TEXT[]),  jsonb_build_object('dml', jsonb_build_object('tableName',r."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME,'sqlStatement',r."TABLE_INFO" ->> 'dml' )), true);
    exception
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO" ->> 'dml' ,'code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end;

  end loop;
  return V_RESULTS;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','call YADAMU_IMPORT_JSON(P_JSON json,P_TARGET_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    return V_RESULTS;
end;  
$$ LANGUAGE plpgsql;
---
create or replace function GENERATE_STATEMENTS(P_METADATA jsonb, P_TYPE_MAPPINGS jsonb, P_TARGET_SCHEMA VARCHAR, P_OPTIONS jsonb)
returns SETOF jsonb
as $$
declare
begin

  call SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS);

  RETURN QUERY
    select jsonb_object_agg(
           "tableName",
           GENERATE_SQL("vendor",P_TARGET_SCHEMA,"tableName","columnNames","dataTypes","sizeConstraints", P_OPTIONS ->> 'spatialFormat', P_OPTIONS ->> 'jsonStorageOption', FALSE)
         )
    from JSONB_EACH(P_METADATA -> 'metadata')  
         CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA(
                                                       "vendor"           VARCHAR,
                                                       "tableSchema"      VARCHAR, 
                                                       "tableName"        VARCHAR, 
                                                       "columnNames"      JSONB, 
                                                       "dataTypes"        JSONB, 
                                                       "sizeConstraints"  JSONB
                                                     );
end;
$$ LANGUAGE plpgsql;
--
select YADAMU_INSTANCE_ID() "YADAMU_INSTANCE_ID", YADAMU_INSTALLATION_TIMESTAMP() "YADAMU_INSTALLATION_TIMESTAMP";
--