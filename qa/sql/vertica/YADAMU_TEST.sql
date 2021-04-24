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
create function YADAMU.XML_DECLARATION()
return VARCHAR
as
begin
  return '<?xml version="1.0"?>';
end;
;
--
create function YADAMU.STRIP_XML_DECLARATION(XML LONG VARCHAR(32000000))
return LONG VARCHAR(32000000)
as
begin
    return case 
	  when SUBSTR(XML,1,LENGTH(YADAMU.XML_DECLARATION())) = YADAMU.XML_DECLARATION() then
	    SUBSTR(XML,LENGTH(YADAMU.XML_DECLARATION())+1)
	  else
	    XML
  end;
end;
;
--