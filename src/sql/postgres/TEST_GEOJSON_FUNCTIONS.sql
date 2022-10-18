with TEST_TABLE as ( 
  select '(1,2)'::point as TEST
)
select TEST, YADAMU.AS__GEOJSON(TEST),YADAMU.AS_POINT(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select '{2,4,5}'::line as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_LINE_EQ(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select '[(2,3),(5,2)]'::lseg as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_LINE_SEGMENT(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select '(5,7),(1,2)'::box as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_BOX(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select ' ((-1,0),(1,0))'::path as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_PATH(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select '((-1,0),(1,0),(4,0))'::path as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_PATH(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select '[(-1,0),(1,0),(4,0)]'::path as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_PATH(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
with TEST_TABLE as ( 
  select ' ((0,0),(1,3),(2,0))'::polygon as TEST
)
select TEST, YADAMU.AS_GEOJSON(TEST),YADAMU.AS_POLYGON(YADAMU.AS_GEOJSON(TEST)) 
from TEST_TABLE;
--
