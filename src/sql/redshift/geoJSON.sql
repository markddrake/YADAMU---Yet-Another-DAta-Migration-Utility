select YADAMU_AsGeoJSON(POINT(1,2));
--
select YADAMU_AsGeoJSON(circle '<(3,2),4>');
--
select YADAMU_AsCircle(YADAMU_AsGeoJSON(circle '<(3,2),4>'));
--
select YADAMU_AsGeoJSON(line '{3,4,5}');
--
select YADAMU_AsLine(YADAMU_AsGeoJSON(line '{3,4,5}'));
--
--
CREATE OR REPLACE FUNCTION YADAMU_AsGEOJSON(point) returns jsonb
as
--
-- POINT
--
$$
select json_build_object(
		 'type',
		 'Point',
		 'coordinates',
		 json_build_array(
		   ($1)[0],
		   ($1)[1]
		 )
	   )
$$
LANGUAGE SQL;
--
CREATE OR REPLACE FUNCTION YADAMU_AsGEOJSON(lseg) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
--
-- LSEG
--
$$
select json_build_object(
         'type',
         'LineString',
         'coordinates',
         json_build_array(
           json_build_array((($1)[0])[0],(($1)[0])[1]),
           json_build_array((($1)[1])[0],(($1)[1])[1])
         )
       )
$$
LANGUAGE SQL;
--
select YADAMU_AsGeoJSON(LSEG(POINT(2,3),POINT(5,2)));
--
CREATE OR REPLACE FUNCTION YADAMU_AsGEOJSON(box) 
returns jsonb
STABLE RETURNS NULL ON NULL INPUT
as
--
-- BOX
--
$$
with POINTS as (
  select YADAMU_asPointArray(polygon($1)) P
)
select json_build_object(
         'type',
         'Polygon',
         'coordinates',
         json_agg(
           json_build_array(
		     json_build_array((POLYGON_POINT)[0],(POLYGON_POINT)[1])
		   )
         )
       )
  from POINTS, unnest(array_append(P,P[1])) POLYGON_POINT
$$
LANGUAGE SQL;
--
select YADAMU_AsGeoJSON(BOX(POINT(1,2),POINT(3,4)));
--
select ST_AsGeoJSON(POLYGON(BOX(POINT(1,2),POINT(3,4)))::geometry);
--
--
-- PATH
--

--
-- POLYGON
--
--

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
select YADAMU_AsLSeg(path '((-1,0),(1,0))');



