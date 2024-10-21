/*
**
** Create the YADAMU Schema
**
*/
--
create schema if not exists YADAMU;
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
    FROM  pg_proc
    WHERE UPPER(pronamespace::regnamespace::text) = 'YADAMU' 
      and UPPER(proname) not in ('YADAMU_INSTANCE_ID','YADAMU_INSTALLATION_TIMESTAMP')
 	  and prokind = 'f'
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed

   if _count > 0 then
     execute _sql;
   end if;
   SELECT count(*)::int
        , 'DROP PROCEDURE ' || string_agg(oid::regprocedure::text, '; DROP PROCEDURE ')
   FROM   pg_proc
   WHERE  UPPER(pronamespace::regnamespace::text) = 'YADAMU'
      and prokind = 'p'
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
** Yadamu Vendor
**
*/
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_VENDOR() 
returns text
STABLE RETURNS NULL ON NULL INPUT
as
$$
select case 
         when version() like '%yugabyte%' then
           'Yugabyte'
		 else 
		   'Postgres'
	    end;
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.POSTGIS_INSTALLED() 
returns boolean
STABLE RETURNS NULL ON NULL INPUT
as
$$
declare
  V_POSTGIS_VERSION    VARCHAR(512);
begin
  SELECT PostGIS_full_version() into V_POSTGIS_VERSION;
  return true;
exception
  when undefined_function then
    return false;
  when others then
    RAISE;
end;
$$ 
LANGUAGE plpgsql;
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
    SELECT YADAMU.YADAMU_INSTANCE_ID() into V_YADAMU_INSTANCE_ID;
  exception
    when undefined_function then
      V_YADAMU_INSTANCE_ID := upper(gen_random_uuid()::CHAR(36));
    when others then
      RAISE;
   end;
   
   COMMAND  := concat('CREATE OR REPLACE FUNCTION YADAMU.YADAMU_INSTANCE_ID() RETURNS CHARACTER VARYING STABLE AS $X$ select ''',V_YADAMU_INSTANCE_ID,''' $X$ LANGUAGE SQL');
   EXECUTE COMMAND;
end $$;
--
do $$ 
declare
   COMMAND                   character varying(256);
   V_INSTALLATION_TIMESTAMP  character varying(27);
begin
  V_INSTALLATION_TIMESTAMP := to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM');
  COMMAND  := concat('CREATE OR REPLACE FUNCTION YADAMU.YADAMU_INSTALLATION_TIMESTAMP() RETURNS CHARACTER VARYING STABLE AS $X$ select ''',V_INSTALLATION_TIMESTAMP,''' $X$ LANGUAGE SQL');
  EXECUTE COMMAND;
end $$;
--
/*
**
** Geometric function needed when PostGIS is not installed
**
*/
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_IS_WKT() 
returns boolean
STABLE RETURNS NULL ON NULL INPUT
as
$$
select true
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_IS_WKB() 
returns boolean
STABLE RETURNS NULL ON NULL INPUT
as
$$
select true
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_IS_GEOJSON() 
returns boolean
STABLE RETURNS NULL ON NULL INPUT
as
$$
select true
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_POINT_ARRAY(text)
/*
**
** Array of Points from open or closed path
**
*/
returns point[]
STABLE RETURNS NULL ON NULL INPUT
as
$$
select array_agg(point(v)) points
  from unnest(string_to_array(left(right($1,-2),-2),'),(')) v
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_PATH(point[],boolean)
--
-- Geneate a Path from an Array of Points
-- 2nd paramter controls whether an open[] path or closed() path is generated: true indicates a closed path
-- Does not check whether the array of points is cyclical
--
returns path
STABLE RETURNS NULL ON NULL INPUT
as
$$
select case
         when ($2) then
           concat('(',string_agg(p::text,','),')')::path
         else 
           concat('[',string_agg(p::text,','),']')::path
		 end 
    from unnest($1) p
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_PATH(lseg)
/*
**
** Convert Line Segment consisting of 2 points into an Open Path object consisting of 2 points
** Enables conversion of lseg to geometry when using PostGIS.
**
*/
returns path
STABLE RETURNS NULL ON NULL INPUT
as
$$
select YADAMU.AS_PATH(array_append(array_append(null::point[],$1[0]),$1[1]),false)
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_LINE_SEGMENT(path) 
/*
**
** Convert Path consisting of 2 points into a Line Segment consisting of 2 points
** Enables conversion of path to lseg when using PostGIS.
*/
returns LSeg
STABLE RETURNS NULL ON NULL INPUT
as
$$
select lseg(p[1],p[2])
  from YADAMU.AS_POINT_ARRAY($1::text) p
$$
LANGUAGE SQL;
--
/*
**
** GeoJSON conversions
**
*/
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(point) 
/*
**
** Convert a Point to GeoJSON Point object
**
*/
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'Point',
         'coordinates',
         jsonb_build_array(
           ($1)[0],
           ($1)[1]
         )
      )
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_POINT(jsonb) 
/*
**
** Convert GeoJSON Point object to a Point
**
*/
returns point
STABLE RETURNS NULL ON NULL INPUT
as
$$
select point(($1 #>> '{coordinates,0}')::double precision,($1 #>> '{coordinates,1}')::double precision)
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(line) 
/*
**
** Convert a LINE equation to a GeoJSON Feature object consisting of a GeoJSON point object and constant property
**
*/
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
CREATE OR REPLACE FUNCTION YADAMU.AS_LINE_EQ(jsonb) 
/*
**
** Convert GeoJSON Feature object consisting of a GeoJSON point object and constant to a LINE equation
**
*/
returns line
STABLE RETURNS NULL ON NULL INPUT
as
$$
select ('{' || ($1 #>> '{geometry,coordinates,0}') || ',' || ($1 #>> '{geometry,coordinates,1}') || ',' || ($1 #>> '{properties,constant}') || '}')::line
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(lseg) 
/*
**
** Convert Line consisting of 2 points into a GEOJSON LineString object
**
*/
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'LineString',
         'coordinates',
         jsonb_build_array(
           jsonb_build_array(
             (($1)[0])[0],
             (($1)[0])[1]
		   ),
           jsonb_build_array(
             (($1)[1])[0],
             (($1)[1])[1]
		   )
        )
      )
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_LINE_SEGMENT(jsonb) 
/*
**
** Convert a GEOJSON LineString object into a Line consisting of 2 points
** LineSegment is defined by the first 2 points in the LineString
** Passing a Line String containing more than 2 points is not valid but is not trapped.
**
*/
returns LSeg
STABLE RETURNS NULL ON NULL INPUT
as
$$
select lseg(point((c #> '{0,0}')::double precision,(c #> '{0,1}')::double precision),point((c #> '{1,0}')::double precision,(c #> '{1,1}')::double precision))
  from jsonb_extract_path($1,'coordinates') c;
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(box) 
/*
**
** Convert a Box consisting of 2 points to a GeoJSON Polygon consisting of 5 ordered points
**
*/
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'Polygon',
         'coordinates',
         jsonb_build_array(
           jsonb_build_array(
             jsonb_build_array(
               case 
			     when ($1[0])[0] < ($1[1])[0] then
			       ($1[0])[0]
                 else 
			       ($1[1])[0] 
			   end,
               case 
			     when ($1[0])[1] < ($1[1])[1] then
			       ($1[0])[1]
                 else 
			       ($1[1])[1]
			   end
			 ),
             jsonb_build_array(
               case 
			     when ($1[0])[0] < ($1[1])[0] then
			       ($1[0])[0]
                 else 
			       ($1[1])[0] 
			   end,
               case 
			     when ($1[0])[1] < ($1[1])[1] then
			       ($1[1])[1]
                 else 
			       ($1[0])[1]
			   end
			 ),
             jsonb_build_array(
               case 
			     when ($1[0])[0] < ($1[1])[0] then
			       ($1[1])[0]
                 else 
			       ($1[0])[0] 
			   end,
               case 
			     when ($1[0])[1] < ($1[1])[1] then
			       ($1[1])[1]
                 else 
			       ($1[0])[1]
			   end
			 ),
             jsonb_build_array(
               case 
			     when ($1[0])[0] < ($1[1])[0] then
			       ($1[1])[0]
                 else 
			       ($1[0])[0] 
			   end,
               case 
			     when ($1[0])[1] < ($1[1])[1] then
			       ($1[0])[1]
                 else 
			       ($1[1])[1]
			   end
			 ),
			 jsonb_build_array(
               case 
			     when ($1[0])[0] < ($1[1])[0] then
			       ($1[0])[0]
                 else 
			       ($1[1])[0] 
			   end,
               case 
			     when ($1[0])[1] < ($1[1])[1] then
			       ($1[0])[1]
                 else 
			       ($1[1])[1]
			   end
			 )
           )
		 )
      )
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_BOX(jsonb) 
/*
**
** Convert a GeoJSON Polygon consisting of 5 ordered points representing a BOX into a box containing the 1st and 3rd points
**
*/
returns box
STABLE RETURNS NULL ON NULL INPUT
as
$$
select box(point(($1 #>> '{coordinates,0,0,0}')::double precision,($1 #>> '{coordinates,0,0,1}')::double precision),point(($1 #>> '{coordinates,0,2,0}')::double precision,($1 #>> '{coordinates,0,2,1}')::double precision))
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_CLOSED_POINT_ARRAY(point[])
/*
**
** Close a point array if it is not already closed
**
*/
returns point[]
STABLE RETURNS NULL ON NULL INPUT
as
$$
select case
         when (($1[1] ~= $1[array_length($1,1)])) then
           $1
         else
           array_append($1,$1[1])
       end
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(path) 
/*
**
** Convert a path to GEOJSON. There are two variants of a path, open[] and closed().
** 
** A open path is mapped to a line string
**
** A Closed Path may need the first pointed appended to the set of points
** 
*/
--
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'LineString',
         'coordinates',
          json_agg(
            json_build_array(p[0],p[1])
          )
       )
       from unnest(case 
	          when isOpen($1) then
	       	    YADAMU.AS_POINT_ARRAY($1::text)
		      else
			    YADAMU.AS_CLOSED_POINT_ARRAY(YADAMU.AS_POINT_ARRAY($1::text))
		      end
			) p
$$            
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(polygon) 
/*
**
** Convert a Polygon to GEOJSON. ##TODO Handle cut-outs ????
**
*/
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select jsonb_build_object(
         'type',
         'Polygon',
         'coordinates',
		 json_build_array(
           json_agg(
             json_build_array(p[0],p[1])
           )
		 )
       )
       from unnest(YADAMU.AS_CLOSED_POINT_ARRAY(YADAMU.AS_POINT_ARRAY($1::text))) p
$$            
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_OPEN_POINT_ARRAY(point[])
/*
**
** Remove the last member from a cyclical array of points
**
*/
returns point[]
STABLE RETURNS NULL ON NULL INPUT
as
$$
select case 
         when ($1[1] ~= $1[array_length($1,1)]) then
		   trim_array($1,1)
         else 
           $1
        end 
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_CLOSED_POINT_ARRAY(point[])
/*
**
** Append the first member of an array to non-cyclical array of points
**
*/
returns point[]
STABLE RETURNS NULL ON NULL INPUT
as
$$
select case 
         when ($1[1] ~= $1[array_length($1,1)]) then
		   $1
         else 
           array_append($1,$1[1])
        end 
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_OPEN_PATH(point[])
/*
**
** Geneate an open[] path from an Array of Points. 
** If the array of points is cyclical the last member 
** of the array is removed before the path is generated 
**
*/
returns path
STABLE RETURNS NULL ON NULL INPUT
as
$$
select concat('[',string_agg(p::text,','),']')::path
  from unnest(YADAMU.AS_OPEN_POINT_ARRAY($1)) p
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_CLOSED_PATH(point[])
/*
**
** Geneate a closed() path from an Array of Points. 
** If the array of points is not cyclical the first member 
** of the array is removed before the path is generated 
**
*/
--
--
returns path
STABLE RETURNS NULL ON NULL INPUT
as
$$
select concat('(',string_agg(p::text,','),')')::path
  from unnest(YADAMU.AS_OPEN_POINT_ARRAY($1)) p
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_PATH(jsonb) 
/*
**
** Geneate a path from a GeoJSON LineString
** If the array of points is cyclical the last member 
** of the array is removed and a closed path is generated 
**
*/
returns path
STABLE RETURNS NULL ON NULL INPUT
as
$$
with POINT_ARRAY 
as (
select array_agg(point((p ->> 0)::double precision,(p ->> 1)::double precision)) points
  from jsonb_array_elements(jsonb_extract_path($1,'coordinates')) p
)
select case
         when (points[1] ~= points[array_length(points,1)]) then
           YADAMU.AS_CLOSED_PATH(points)
         else 
		   YADAMU.AS_OPEN_PATH(points)
       end
  from POINT_ARRAY
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_POLYGON(jsonb) 
returns polygon
STABLE RETURNS NULL ON NULL INPUT
as
$$
with POINT_ARRAY 
as (
select array_agg(point((p ->> 0)::double precision,(p ->> 1)::double precision)) points
  from jsonb_array_elements(jsonb_extract_path($1,'coordinates','0')) p
)
select polygon(YADAMU.AS_CLOSED_PATH(points))
  from POINT_ARRAY
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_GEOJSON(circle) 
/*
**
** Convert a Circle to a GeoJSON Feature object consisting of a GeoJSON point object and a radius property
**
*/
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
CREATE OR REPLACE FUNCTION YADAMU.AS_CIRCLE(jsonb) 
/*
**
** Convert GeoJSON Feature object consisting of a GeoJSON point object and radius to a Circle
**
*/
returns circle
STABLE RETURNS NULL ON NULL INPUT
as
$$
select circle(point(($1 #>> '{geometry,coordinates,0}')::double precision,($1 #>> '{geometry,coordinates,1}')::double precision),($1 #>> '{properties,radius}')::double precision)
$$
LANGUAGE SQL;
--
/*
**
** JSON Based Export/Import of TSVECTOR
**
*/
--
CREATE OR REPLACE FUNCTION YADAMU.AS_JSON(tsvector) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
$$
select array_to_json(tsvector_to_array($1))::jsonb
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU.AS_TS_VECTOR(jsonb) 
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
CREATE OR REPLACE FUNCTION YADAMU.AS_JSON(anyRange) 
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
CREATE OR REPLACE FUNCTION YADAMU.AS_RANGE(jsonb)
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
/*
**
** YADAMU_EXPORT Function.
**
*/
--
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_EXPORT(P_SCHEMA VARCHAR, P_SPATIAL_FORMAT VARCHAR, P_OPTIONS JSONB DEFAULT '{"circleAsPolygon": false}'::JSONB) 
returns TABLE ( TABLE_SCHEMA VARCHAR, TABLE_NAME VARCHAR, COLUMN_NAME_ARRAY JSONB, DATA_TYPE_ARRAY JSONB, SIZE_CONSTRAINT_ARRAY JSONB, CLIENT_SELECT_LIST TEXT, ERRORS JSONB)
as $$
declare
  R                       RECORD;
  V_SQL_STATEMENT         TEXT = NULL;
  PLPGSQL_CTX             TEXT;
  C_CHARACTER_MAX_LENGTH  int = 1*1024*1024*1024;
  V_SIZE_CONSTRAINTS      JSONB;
  V_POSTGIS_INSTALLED     BOOLEAN := YADAMU.POSTGIS_INSTALLED();
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
                                jsonb_build_array(c.numeric_precision,c.numeric_scale)
                              when (c.numeric_precision is not null) then 
                                jsonb_build_array(c.numeric_precision)
                              when (c.character_maximum_length is not null) then 
                                jsonb_build_array(c.character_maximum_length)
                              when (c.datetime_precision is not null) then 
                                jsonb_build_array(c.datetime_precision)
                              when ((c.data_type in ('character','character varying','text','xml')) and (c.character_maximum_length is null)) then
                                jsonb_build_array(C_CHARACTER_MAX_LENGTH)
                              else
                                jsonb_build_array()
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
                               when ((c.data_type = 'USER-DEFINED') and (c.udt_name in ('geometry')) and V_POSTGIS_INSTALLED)  then
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
                                 end
                               when ((c.data_type = 'USER-DEFINED') and (c.udt_name in ('geography')) and V_POSTGIS_INSTALLED)  then
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
                                 end
                               when (c.data_type in ('point','path','polygon') and V_POSTGIS_INSTALLED) then
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
                                 end
                               when ((c.data_type = 'lseg') and V_POSTGIS_INSTALLED) then
                                 case
                                   when P_SPATIAL_FORMAT = 'WKB' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsBinary(YADAMU.AS_PATH("' || column_name || '")::geometry) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'WKT' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsText(YADAMU.AS_PATH("' || column_name || '")::geometry,18) end "' || COLUMN_NAME || '"' 
                                   when P_SPATIAL_FORMAT = 'EWKB' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsEWKB(YADAMU.AS_PATH("' || column_name || '")::geometry) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'EWKT' then
                                     'case when "' || column_name || '" is NULL then NULL else ST_AsEWKT(YADAMU.AS_PATH("' || column_name || '")::geometry) end "' || COLUMN_NAME || '"'
                                   when P_SPATIAL_FORMAT = 'GeoJSON' then
                                      'case when "' || column_name || '" is NULL then NULL else ST_AsGeoJSON(YADAMU.AS_PATH("' || column_name || '"))::geometry) end "' || COLUMN_NAME || '"'
                                 end
                               when ((c.data_type = 'box') and V_POSTGIS_INSTALLED) then
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
                                 end
                               when ((c.data_type = 'circle') and V_POSTGIS_INSTALLED) then
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
                                     end
                                   else 
                                     'YADAMU.AS_GEOJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                 end
                               when (c.data_type in ('point','line','lseg','box','path','polygon')) then
                                 'YADAMU.AS_GEOJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                               when (c.data_type = 'circle') then
                                  case 
                                    when ((P_OPTIONS ->> 'circleAsPolygon')::boolean = true) then
                                      'YADAMU.AS_GEOJSON(polygon(32,"' || column_name || '")) "' || COLUMN_NAME || '"'
                                    else 
                                      'YADAMU.AS_GEOJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                  end  
                               when (c.data_type in ('int4range','int8range','numrange','tsrange','tstzrange','daterange','tsvector')) then
                                 'YADAMU.AS_JSON("' || column_name || '") "' || COLUMN_NAME || '"'
                               when (c.data_type in ('real')) then 
                                 '"' || column_name || '"::double precision "' || COLUMN_NAME || '"'
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
    ** Calculate the max length of bytea fields, since Postgres based databases do not maintain a maximum length in the dictionary.
    **
    */
    select 'select jsonb_build_array(' 
        || string_agg(
             case
               when d.value = 'bytea' then 
                 'case when max(octet_length("' || c.value || '")) is null then jsonb_build_array() else jsonb_build_array(max(octet_length("' || c.value || '"))) end'
               else
                 '''' || s.value || '''::jsonb'
             end
            ,','
           ) 
        || ') FROM "' || r."TABLE_SCHEMA" || '"."' || r."TABLE_NAME" || '"'
      from jsonb_array_elements_text(r."COLUMN_NAME_ARRAY") WITH ORDINALITY as c(value, idx)
      join jsonb_array_elements_text(r."DATA_TYPE_ARRAY")   WITH ORDINALITY as d(value, idx) on c.idx = d.idx
      join jsonb_array_elements_text(r."SIZE_CONSTRAINT_ARRAY")  WITH ORDINALITY as s(value, idx) on c.idx = s.idx
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
** YADAMU_IMPORT_JSON Function.
**
*/
--
CREATE OR REPLACE PROCEDURE YADAMU. SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS JSONB) 
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
CREATE OR REPLACE FUNCTION YADAMU.MAP_PGSQL_DATA_TYPE(P_VENDOR VARCHAR, P_PGSQL_DATA_TYPE  VARCHAR, P_DATA_TYPE VARCHAR, P_DATA_TYPE_LENGTH BIGINT, P_DATA_TYPE_SCALE INT, P_JSON_DATA_TYPE VARCHAR) 
returns VARCHAR
as $$
declare
  C_CLOB_TYPE                         VARCHAR(32) = 'text';
  C_ORACLE_OBJECT_TYPE                VARCHAR(32) = C_CLOB_TYPE;
  C_UNBOUNDED_NUMERIC                 VARCHAR(32) = 'numeric';
  C_NUMERIC_PRECISION                   SMALLINT = 1000;
  C_NUMERIC_SCALE                       SMALLINT = 1000;
  
begin
  case 
    when ((P_PGSQL_DATA_TYPE is null) and (P_VENDOR = 'Oracle') and (P_DATA_TYPE like '"%"."%"')) then
      return C_ORACLE_OBJECT_TYPE;
    when P_PGSQL_DATA_TYPE is null then
      raise exception '%: Missing mapping for "%" datatype "%"', YADAMU.YADAMU_VENDOR(), P_VENDOR, P_DATA_TYPE;
    when P_PGSQL_DATA_TYPE = 'numeric'                                then return 
    case 
      when P_DATA_TYPE_LENGTH is NULL                                 then C_UNBOUNDED_NUMERIC 
      when ((P_DATA_TYPE_LENGTH > C_NUMERIC_PRECISION) 
       and (P_DATA_TYPE_SCALE  > C_NUMERIC_SCALE))                    then C_UNBOUNDED_NUMERIC
      when P_DATA_TYPE_LENGTH > C_NUMERIC_PRECISION                   then concat(P_PGSQL_DATA_TYPE,'(',cast(C_NUMERIC_PRECISION as CHAR(4)),',',cast(P_DATA_TYPE_SCALE as CHAR(4)),')')
       when P_DATA_TYPE_SCALE > C_NUMERIC_SCALE                       then concat(P_PGSQL_DATA_TYPE,'(',cast(P_DATA_TYPE_LENGTH as CHAR(4)),',',cast(C_NUMERIC_SCALE as CHAR(4)),')')
                                                                      else P_PGSQL_DATA_TYPE 
    end;    
                                                                      else return P_PGSQL_DATA_TYPE;
  end case;
end;  
$$ LANGUAGE plpgsql;
--
CREATE OR REPLACE FUNCTION YADAMU.GENERATE_SQL(P_VENDOR VARCHAR,P_TARGET_SCHEMA VARCHAR, P_TABLE_NAME VARCHAR, P_COLUMN_NAME_ARRAY JSONB, P_DATA_TYPE_ARRAY JSONB, P_SIZE_CONSTRAINT_ARRAY JSONB, P_SPATIAL_FORMAT VARCHAR, P_JSON_DATA_TYPE VARCHAR, P_BINARY_JSON BOOLEAN)
returns JSONB
as $$
declare
  V_COLUMN_LIST        TEXT;
  V_COLUMNS_CLAUSE     TEXT;
  V_INSERT_SELECT_LIST TEXT;
  V_TARGET_DATA_TYPES  JSONB;
  C_MAX_BOUNDED_LENGTH INT = 10 * 1024 * 1024;
begin

  with
  SOURCE_TABLE_DEFINITIONS
  as (
    select c.IDX
          ,c.VALUE COLUMN_NAME
          ,t.VALUE DATA_TYPE
          ,case 
             when P_VENDOR =  YADAMU.YADAMU_VENDOR() then
               t.VALUE 
             else 
               m."PGSQL_TYPE"
           end "PGSQL_TYPE"
          ,cast(s.VALUE ->> 0 as BIGINT) DATA_TYPE_LENGTH
          ,cast(s.VALUE ->> 1 as BIGINT) DATA_TYPE_SCALE
      from JSONB_ARRAY_ELEMENTS_TEXT(P_COLUMN_NAME_ARRAY)     WITH ORDINALITY as c(VALUE, IDX)
      join JSONB_ARRAY_ELEMENTS_TEXT(P_DATA_TYPE_ARRAY)       WITH ORDINALITY as t(VALUE, IDX) on c.IDX = t.IDX
      join JSONB_ARRAY_ELEMENTS(P_SIZE_CONSTRAINT_ARRAY)      WITH ORDINALITY as s(VALUE, IDX) on c.IDX = s.IDX
      left outer join TYPE_MAPPING m on lower(t.VALUE) = lower(m."VENDOR_TYPE")
      -- left outer join TYPE_MAPPING m on t.VALUE = m."VENDOR_TYPE"
  ),
  TARGET_TABLE_DEFINITIONS
  as (
    select st.*,
           YADAMU.MAP_PGSQL_DATA_TYPE(P_VENDOR,"PGSQL_TYPE",DATA_TYPE,DATA_TYPE_LENGTH::BIGINT,DATA_TYPE_SCALE::INT, P_JSON_DATA_TYPE) TARGET_DATA_TYPE
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
                    ,'dml', 'INSERT into "' || P_TARGET_SCHEMA || '"."' || P_TABLE_NAME || '"(' || V_COLUMN_LIST || ') OVERRIDING SYSTEM VALUE ' || CHR(10) || 'select ' || V_INSERT_SELECT_LIST || CHR(10) || '  from ' || case WHEN P_BINARY_JSON then 'jsonb_array_elements' else 'json_array_elements' end || '($1 -> ''data'' -> ''' || P_TABLE_NAME || ''')'
                    ,'targetDataTypes', V_TARGET_DATA_TYPES
               );
end;  
$$ LANGUAGE plpgsql;
--
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_IMPORT_JSONB(P_JSON jsonb, P_TYPE_MAPPINGS jsonb, P_TARGET_SCHEMA VARCHAR, P_OPTIONS jsonb) 
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

  call YADAMU.SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS);

  for r in select "tableName"
                 ,YADAMU.GENERATE_SQL(P_JSON #>> '{systemInformation,vendor}',P_TARGET_SCHEMA,"tableName", "columnNames", "dataTypes", "sizeConstraints", P_JSON #>> '{systemInformatio,driverSettings,spatialFormat}', P_OPTIONS ->> 'jsonStorageOption', TRUE) "TABLE_INFO"
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
CREATE OR REPLACE FUNCTION YADAMU.YADAMU_IMPORT_JSON(P_JSON json, P_TYPE_MAPPINGS jsonb, P_TARGET_SCHEMA VARCHAR, P_OPTIONS jsonb) 
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

  call YADAMU.SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS);

  for r in select "tableName"
                 ,YADAMU.GENERATE_SQL(P_JSON #>> '{systemInformation,vendor}',P_TARGET_SCHEMA,"tableName","columnNames","dataTypes","sizeConstraints", P_JSON #>> '{systemInformation,driverSettings,spatialFormat}', P_OPTIONS ->> 'jsonStorageOption', FALSE) "TABLE_INFO"
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
--
CREATE OR REPLACE FUNCTION YADAMU.GENERATE_STATEMENTS(P_METADATA jsonb, P_TYPE_MAPPINGS jsonb, P_TARGET_SCHEMA VARCHAR, P_OPTIONS jsonb)
returns SETOF jsonb
as $$
declare
begin

  call YADAMU.SET_VENDOR_TYPE_MAPPINGS(P_TYPE_MAPPINGS);

  RETURN QUERY
    select jsonb_object_agg(
           "tableName",
           YADAMU.GENERATE_SQL("vendor",P_TARGET_SCHEMA,"tableName","columnNames","dataTypes","sizeConstraints", P_OPTIONS ->> 'spatialFormat', P_OPTIONS ->> 'jsonStorageOption', FALSE)
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
select YADAMU.YADAMU_INSTANCE_ID() "YADAMU_INSTANCE_ID", YADAMU.YADAMU_INSTALLATION_TIMESTAMP() "YADAMU_INSTALLATION_TIMESTAMP";
--