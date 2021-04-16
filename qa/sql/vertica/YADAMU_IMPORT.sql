create or replace function YADAMU.invalidGeoHash(c GEOMETRY) 
return BOOLEAN
as 
begin
  return ((ST_Xmax(c) > 180) or (ST_Xmin(c) < -180) or (ST_Ymax(c) > 90) or (ST_Ymin(c) < -90));
end;
--
create or replace function YADAMU.invalidGeoHash(c GEOGRAPHY) 
return BOOLEAN
as 
begin
  return ((ST_Xmax(c) > 180) or (ST_Xmin(c) < -180) or (ST_Ymax(c) > 90) or (ST_Ymin(c) < -90));
end;
--