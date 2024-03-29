--
CREATE OR REPLACE FUNCTION YADAMU.APPLY_XML_RULE (p_xml_rule VARCHAR, p_serialized_xml TEXT)   
RETURNS text
AS $$
import lxml.etree as XML

stylesheets = {
  "SNOWFAKE_VARIANT" : """<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:strip-space elements="*"/>
	<xsl:template match="node()">
		<xsl:copy>
			<xsl:for-each select="@*">
				<xsl:sort order="ascending" select="name()"/>
				<xsl:attribute name="{name(.)}" namespace="{namespace-uri(.)}">
     	          <xsl:choose>
				    <xsl:when test="number(.) = number(.)">
			          <xsl:value-of select="number(.)"/>
		            </xsl:when>
		            <xsl:otherwise>
			          <xsl:value-of select="."/>
		            </xsl:otherwise>
		          </xsl:choose>
				</xsl:attribute>
			</xsl:for-each>
			<xsl:apply-templates select="node()"/>
		</xsl:copy>
	</xsl:template>
	<xsl:template match="comment()"/>
	<xsl:template match="processing-instruction()" />
	<xsl:template match="text()">
		<xsl:choose>
			<xsl:when test="string-length(normalize-space(.))=0">
				<xsl:value-of select="."/>
			</xsl:when>
			<xsl:otherwise>
				<xsl:call-template name="trim">
					<xsl:with-param name="str" select="."/>
				</xsl:call-template>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
	<xsl:template name="trim">
		<xsl:param name="str"/>
	    <xsl:variable name="leftTrim" select="string-length(normalize-space(substring($str,1,1)))=0"/>
	    <xsl:variable name="rightTrim" select="string-length(normalize-space(substring($str,string-length($str),1)))=0"/>
	    <xsl:choose>
	      <xsl:when test="$leftTrim and $rightTrim">
			<xsl:call-template name="trim">
				<xsl:with-param name="str" select="substring($str,2,string-length($str)-1)"/>
			</xsl:call-template>
	      </xsl:when>
	      <xsl:when test="$leftTrim">
			<xsl:call-template name="trim">
				<xsl:with-param name="str" select="substring($str,2,string-length($str))"/>
			</xsl:call-template>
	      </xsl:when>
	      <xsl:when test="$rightTrim">
			<xsl:call-template name="trim">
				<xsl:with-param name="str" select="substring($str,1,string-length($str)-1)"/>
			</xsl:call-template>
	      </xsl:when>
     	  <xsl:when test="number($str) = number($str)">
			<xsl:value-of select="number($str)"/>
		  </xsl:when>
		  <xsl:otherwise>
			 <xsl:value-of select="$str"/>
		  </xsl:otherwise>
		</xsl:choose>
	</xsl:template>
</xsl:stylesheet>"""
, "STRIP_XML_DECLARATION" : """<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" omit-xml-declaration="yes" />
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>"""
};

dom        = XML.fromstring(p_serialized_xml);

xslt = XML.fromstring(stylesheets[p_xml_rule]);
transform  = XML.XSLT(xslt);
newdom     = transform(dom);
 
return XML.tostring(newdom, pretty_print=False);

$$ LANGUAGE plpython3u;
--
create or replace function YADAMU.TRUNCATE_GEOMETRY_WKT(P_GEOMETRY GEOMETRY,P_SPATIAL_PRECISION INT) 
returns TEXT
as $$

  declare X  NUMERIC(28,18);
  declare Y  NUMERIC(28,18);

  declare WKT TEXT;

  declare RING_NUMBER INT := 0;
  declare RING_COUNT INT := 0;
  declare RING_SEPERATOR CHAR(1) := '';
  declare POINT_id INT := 0;
  declare POINT_NUMBER INT := 0;
  declare POINT_COUNT INT := 0;
  declare POINT_SEPERATOR CHAR(1) := '';
  declare POLYGON_NUMBER INT := 0;
  declare POLYGON_COUNT INT := 0;
  declare POLYGON_SEPERATOR CHAR(1) := '';

  declare RING GEOMETRY;
  declare POINT GEOMETRY;
  declare POLYGON GEOMETRY;
BEGIN
   WKT := GeometryType(P_GEOMETRY) ;

  if (GeometryType(P_GEOMETRY) = 'POINT') then
    X := trunc(ST_X(P_GEOMETRY)::NUMERIC(28,18),P_SPATIAL_PRECISION);
	Y := trunc(ST_Y(P_GEOMETRY)::NUMERIC(28,18),P_SPATIAL_PRECISION);
    WKT := concat('POINT(',X,' ',Y,')');
    return WKT;
  end if;
  
  if (GeometryType(P_GEOMETRY) = 'POLYGON') then
    WKT := concat(WKT,'(');
    RING_NUMBER := 0;
    RING := ST_ExteriorRing(P_GEOMETRY);
    POINT_NUMBER := 0;
    POINT_COUNT = ST_NPoints(RING);
    WKT := concat(WKT,'(');
    while (POINT_NUMBER < POINT_COUNT) loop
      POINT_NUMBER := POINT_NUMBER + 1;
      POINT = ST_PointN(RING,POINT_NUMBER);
      X := trunc(ST_X(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
	  Y := trunc(ST_Y(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
	  WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
      POINT_SEPERATOR = ',';
    end loop;
    WKT := concat(WKT,')');  
    RING_NUMBER := RING_NUMBER + 1;
    RING_SEPERATOR := ',';
    RING_COUNT := ST_NRings(P_GEOMETRY);
    while (RING_NUMBER  < RING_COUNT) loop
      WKT := concat(WKT,RING_SEPERATOR,'(');
      RING := ST_InteriorRingN(P_GEOMETRY,RING_NUMBER);
      RING_NUMBER := RING_NUMBER + 1;
      POINT_NUMBER := 0;
      POINT_SEPERATOR = ' ';
      POINT_COUNT = ST_NPoints(RING);
      while (POINT_NUMBER < POINT_COUNT) loop
        POINT_NUMBER := POINT_NUMBER + 1;
        POINT = ST_PointN(RING,POINT_NUMBER);
        X := trunc(ST_X(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
	    Y := trunc(ST_Y(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
  	    WKT = concat(WKT,POINT_SEPERATOR,X,' ',Y);
        POINT_SEPERATOR = ',';
      end loop;
      WKT := concat(WKT,')');  
    end loop; 
    WKT := concat(WKT,')');  
    return WKT;
  end if;

  if (GeometryType(P_GEOMETRY) = 'MULTIPOLYGON') then
    WKT = concat(WKT,'(');
    POLYGON_NUMBER := 0;
    POLYGON_SEPERATOR := '';
    POLYGON_COUNT := ST_NumGeometries(P_GEOMETRY);
    while (POLYGON_NUMBER  < POLYGON_COUNT) loop
      WKT := concat(WKT,POLYGON_SEPERATOR,'(');
      POLYGON_SEPERATOR := ',';      
      POLYGON_NUMBER = POLYGON_NUMBER + 1;
      POLYGON = ST_GeometryN(P_GEOMETRY,POLYGON_NUMBER);
      RING_NUMBER := 0;
      RING := ST_ExteriorRing(POLYGON);
      POINT_NUMBER := 0;
      POINT_SEPERATOR = ' ';
      POINT_COUNT = ST_NPoints(RING);
      WKT = concat(WKT,'(');
      while (POINT_NUMBER < POINT_COUNT) loop
        POINT_NUMBER := POINT_NUMBER + 1;
        POINT = ST_PointN(RING,POINT_NUMBER);
        X := trunc(ST_X(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
	    Y := trunc(ST_Y(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
        WKT := concat(WKT,POINT_SEPERATOR,X,' ',Y);
        POINT_SEPERATOR = ',';
      end loop;
      WKT := concat(WKT,')');
      RING_NUMBER := RING_NUMBER + 1;
      RING_COUNT := ST_NRings(POLYGON);
      RING_SEPERATOR := ',';
      while (RING_NUMBER  < RING_COUNT) loop
        WKT := concat(WKT,RING_SEPERATOR,'(');
        RING := ST_InteriorRingN(POLYGON,RING_NUMBER);
        RING_NUMBER := RING_NUMBER + 1;
        POINT_NUMBER := 0;
        POINT_SEPERATOR = ' ';
        POINT_COUNT := ST_NPoints(RING);
        while (POINT_NUMBER < POINT_COUNT) loop
          POINT_NUMBER := POINT_NUMBER + 1;
          POINT := ST_PointN(RING,POINT_NUMBER);
          X := trunc(ST_X(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
	      Y := trunc(ST_Y(POINT)::NUMERIC(28,18),P_SPATIAL_PRECISION);
          WKT := concat(WKT,POINT_SEPERATOR,X,' ',Y);
          POINT_SEPERATOR = ',';
        end loop;
        WKT := concat(WKT,')');
      end loop;
      WKT := concat(WKT,')');
    end loop;
    WKT := concat(WKT,')');
    return WKT;
  end if;
  return ST_AsTEXT(P_GEOMETRY);
END; $$ 
LANGUAGE 'plpgsql';
--
CREATE OR REPLACE FUNCTION YADAMU.GENERATE_COMPARE_COLUMNS(P_SOURCE_SCHEMA VARCHAR, P_COMPARE_RULES JSONB) 
returns TABLE ( TABLE_NAME VARCHAR, COLUMN_LIST TEXT, ALT_COLUMN_LIST TEXT)
as $$
declare
  R RECORD;

  V_EMPTY_STRING_IS_NULL BOOLEAN       = '{"emptyStringIsNull": true}'::jsonb <@ P_COMPARE_RULES;
  V_MIN_BIGINT_IS_NULL   BOOLEAN       = '{"minBigIntIsNull": true}'::jsonb <@ P_COMPARE_RULES;																							   
  V_INFINITY_IS_NULL     BOOLEAN       = '{"infinityIsNull": true}'::jsonb <@ P_COMPARE_RULES;
  V_SPATIAL_PRECISION    INT           = P_COMPARE_RULES ->> 'spatialPrecision';
  V_DOUBLE_PRECISION     INT           = P_COMPARE_RULES ->> 'doublePrecision';
  V_NUMERIC_SCALE        INT           = P_COMPARE_RULES ->> 'numericScale';
  V_XML_RULE             VARCHAR(32)   = P_COMPARE_RULES ->> 'xmlRule';

begin

  for r in select t.table_name
	             ,string_agg(
                    case 
                      when data_type in ('character varying','text') then
                        case 
                          when V_EMPTY_STRING_IS_NULL then
                            'case when"' || column_name || '" = '''' then NULL else "' || column_name || '" end "' || column_name || '"' 
                          else 
                            '"' || column_name || '"' 
                        end
                      when data_type in ('bigint') then
                        case 
                          when V_MIN_BIGINT_IS_NULL then
                            'case when"' || column_name || '" = -9223372036854775808 then NULL else "' || column_name || '" end "' || column_name || '"' 
                          else 
                            '"' || column_name || '"' 
                        end
                      when data_type in ('real','double precision') then
                        case 
                          when V_INFINITY_IS_NULL then
                            'case when"' || column_name || '" in (''Infinity'',''-Infinity'',''NaN'') then NULL else "' || column_name || '" end "' || column_name || '"' 
                          else 
						    case 
							  when (V_DOUBLE_PRECISION < 18) then
							   'round("' || column_name || '"::numeric,' || V_DOUBLE_PRECISION || ') "' || column_name || '"'
							  else
                                '"' || column_name || '"' 
					        end
                        end
					  when data_type in ('numeric','decimal') then
                        case 
   					      when  (((numeric_scale is NULL) and (V_NUMERIC_SCALE is not NULL)) or (V_NUMERIC_SCALE < numeric_scale)) then 
							'round("' || column_name || '",' || V_NUMERIC_SCALE || ') "' || column_name || '"'
						  else
                            'trim_scale("' || column_name || '") "' || column_name || '"' 
                        end
                      when data_type = 'json'  then
					   -- ### TODO: Size restiction on this conversion ????
                        'to_jsonb("' || column_name || '")::text' 
                      when data_type = 'xml' then
					     case
					       when (V_XML_RULE is NOT NULL) then
 	           	             'case when "' || column_name || '" is null then null else YADAMU.APPLY_XML_RULE(''' || V_XML_RULE ||''',"' || column_name || '"::text) end' 
                           else
                             '"' || column_name || '"::text' 
                         end
                      when ((data_type = 'USER-DEFINED') and (udt_name = 'geometry')) then
                       case 
                         when V_SPATIAL_PRECISION < 18 then
                           'ST_AsText("' || column_name || '",' || V_SPATIAL_PRECISION || ')' 
                         else
                           'ST_AsEWKB("' || column_name || '")' 
                        end
                      when ((data_type = 'USER-DEFINED') and (udt_name = 'geography')) then
                       case 
                         when V_SPATIAL_PRECISION < 18 then
                           'ST_AsText("' || column_name || '",' || V_SPATIAL_PRECISION || ')' 
                         else
                           'ST_AsBinary("' || column_name || '")' 
                        end
                      when (data_type in ('point','path','polygon') and YADAMU.POSTGIS_INSTALLED()) then
                       case 
                         when V_SPATIAL_PRECISION < 18 then
                           'ST_AsText("' || column_name || '"::geometry,' || V_SPATIAL_PRECISION || ')'
                         else
                           'ST_AsBinary("' || column_name || '"::geometry)' 
                        end
                      when (data_type in ('line','lseg','box','circle','point','path','polygon')) then
                        '"' || column_name || '"::text' 
                      when (data_type in ('refcursor','txid_snapshot','gtsvector')) then
                        '"' || column_name || '"::text' 
                      when (data_type in ('tid','xid','cid') and (current_setting('server_version_num')::integer < 120000)) then
                        '"' || column_name || '"::text' 
                      else 
                        '"' || column_name || '"' 
                    end
                   ,',' 
                   order by ordinal_position
                  ) COLUMN_LIST
	             ,string_agg(
                    case 
                      when data_type in ('character varying','text') then
                        case 
                          when V_EMPTY_STRING_IS_NULL then
                            'case when"' || column_name || '" = '''' then NULL else "' || column_name || '" end "' || column_name || '"' 
                          else 
                            '"' || column_name || '"' 
                        end
                      when data_type in ('real','double precision') then
                        case 
                          when V_INFINITY_IS_NULL then
                            'case when"' || column_name || '" in (''Infinity'',''-Infinity'',''NaN'') then NULL else "' || column_name || '" end "' || column_name || '"' 
                          else 
						    case 
							  when (V_DOUBLE_PRECISION < 18) then
							   'round("' || column_name || '"::numeric,' || V_DOUBLE_PRECISION || ') "' || column_name || '"'
							  else
                                '"' || column_name || '"' 
					        end
                        end
                      when data_type = 'json'  then
                        '"' || column_name || '"::text' 
                      when data_type = 'xml' then
					     case
					       when (V_XML_RULE is NOT NULL) then
 	           	             'case when "' || column_name || '" is null then null else YADAMU.APPLY_XML_RULE(''' || V_XML_RULE ||''',"' || column_name || '"::text) end' 
                           else
                             '"' || column_name || '"::text' 
                         end
                      when ((data_type = 'USER-DEFINED') and (udt_name = 'geometry')) then
                        'YADAMU.TRUNCATE_GEOMETRY_WKT("' || column_name || '",' || V_SPATIAL_PRECISION || ')' 
                      when ((data_type = 'USER-DEFINED') and (udt_name = 'geography')) then
                        'YADAMU.TRUNCATE_GEOMETRY_WKT("' || column_name || '"::geometry,' || V_SPATIAL_PRECISION || ')' 
                      when (data_type in ('point','path','polygon') and YADAMU.POSTGIS_INSTALLED()) then
                       case 
                         when V_SPATIAL_PRECISION < 18 then
                           'ST_AsText("' || column_name || '"::geometry,' || V_SPATIAL_PRECISION || ')'
                         else
                           'ST_AsBinary("' || column_name || '"::geometry)' 
                        end
                      when (data_type in ('line','lseg','box','circle','point','path','polygon')) then
                        '"' || column_name || '"::text' 
                      when (data_type in ('refcursor','txid_snapshot','gtsvector')) then
                        '"' || column_name || '"::varchar' 
                      when (data_type in ('tid','xid','cid') and (current_setting('server_version_num')::integer < 120000)) then
                        '"' || column_name || '"::text' 
                      else 
                        '"' || column_name || '"' 
                    end
                   ,',' 
                   order by ordinal_position
                  ) ALT_COLUMN_LIST
             from information_schema.columns c, information_schema.tables t
            where t.table_name = c.table_name 
              and t.table_schema = c.table_schema
	          and t.table_type = 'BASE TABLE'
              and t.table_schema = P_SOURCE_SCHEMA
            group by t.table_schema, t.table_name 

  loop
    TABLE_NAME = r.TABLE_NAME;
	COLUMN_LIST = r.COLUMN_LIST;
	ALT_COLUMN_LIST = r.ALT_COLUMN_LIST;
	return NEXT;
  end loop;
end;
$$ LANGUAGE plpgsql;
	
    
create or replace procedure YADAMU.COMPARE_SCHEMA(P_SOURCE_SCHEMA VARCHAR,P_TARGET_SCHEMA VARCHAR, P_COMPARE_RULES JSONB)
as $$
declare

  R RECORD;
  V_SQL_STATEMENT        TEXT;
  C_NEWLINE              CHAR(1) = CHR(10);
  
  V_SOURCE_COUNT         INT;
  V_TARGET_COUNT         INT;
  V_MISSING_ROWS         INT;
  V_EXTRA_ROWS           INT;
  V_SQLERRM              TEXT;

begin

  -- RAISE INFO 'Spatial Precision : %', V_SPATIAL_PRECISION ;
  
  create temporary table if not exists SCHEMA_COMPARE_RESULTS (
    SOURCE_SCHEMA    VARCHAR(128)
   ,TARGET_SCHEMA    VARCHAR(128)
   ,TABLE_NAME       VARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSING_ROWS     INT
   ,EXTRA_ROWS       INT
   ,SQLERRM          TEXT
  );

  TRUNCATE TABLE SCHEMA_COMPARE_RESULTS;
  
    for r in select * from YADAMU.GENERATE_COMPARE_COLUMNS(P_SOURCE_SCHEMA,P_COMPARE_RULES) loop
    begin
      V_SQL_STATEMENT := concat('select count(*) from "',P_SOURCE_SCHEMA,'"."',r.TABLE_NAME,'"');
      EXECUTE V_SQL_STATEMENT into V_SOURCE_COUNT;
   
      V_SQL_STATEMENT := concat('select count(*) from "',P_TARGET_SCHEMA,'"."',r.TABLE_NAME,'"');
      EXECUTE V_SQL_STATEMENT into V_TARGET_COUNT;

      V_SQL_STATEMENT := concat('select count(*) from (SELECT ' || r.COLUMN_LIST || ' from "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '" EXCEPT SELECT ' || r.COLUMN_LIST || ' from  "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '") "T"');
      -- RAISE INFO 'SQL : %', V_SQL_STATEMENT ;
      EXECUTE V_SQL_STATEMENT into V_MISSING_ROWS;

      V_SQL_STATEMENT := concat('select count(*) from (SELECT ' || r.COLUMN_LIST || ' from "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '" EXCEPT SELECT ' || r.COLUMN_LIST || ' from  "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '") "T"');
      EXECUTE V_SQL_STATEMENT into V_EXTRA_ROWS;

	  if ((V_MISSING_ROWS > 0) and (V_EXTRA_ROWS > 0) and (V_MISSING_ROWS = V_EXTRA_ROWS)) then
        -- RAISE INFO 'Mismatch: % % %', r.TABLE_NAME, V_MISSING_ROWS, V_EXTRA_ROWS;
        V_SQL_STATEMENT := concat('select count(*) from (SELECT ' || r.ALT_COLUMN_LIST || ' from "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '" EXCEPT SELECT ' || r.ALT_COLUMN_LIST || ' from  "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '") "T"');
        -- RAISE INFO 'SQL : %', V_SQL_STATEMENT ;
        EXECUTE V_SQL_STATEMENT into V_MISSING_ROWS;  

        V_SQL_STATEMENT := concat('select count(*) from (SELECT ' || r.ALT_COLUMN_LIST || ' from "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '" EXCEPT SELECT ' || r.ALT_COLUMN_LIST || ' from  "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '") "T"');
        EXECUTE V_SQL_STATEMENT into V_EXTRA_ROWS;
       -- RAISE INFO 'Truncated: % % %', r.TABLE_NAME, V_MISSING_ROWS, V_EXTRA_ROWS;        
	  end if;

      insert into SCHEMA_COMPARE_RESULTS VALUES (P_SOURCE_SCHEMA, P_TARGET_SCHEMA, r.TABLE_NAME, V_SOURCE_COUNT, V_TARGET_COUNT, V_MISSING_ROWS, V_EXTRA_ROWS, NULL);
    exception  
      when others then
        V_SQLERRM = SQLERRM;
        V_SOURCE_COUNT = -1;
        V_TARGET_COUNT = -1;

        begin 
          EXECUTE 'select count(*) from "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '"' into V_SOURCE_COUNT;
        exception 
          when others then
            null;
        end;
         
        begin 
          EXECUTE 'select count(*) from "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '"' into V_TARGET_COUNT;
        exception 
          when others then
            null;
        end;
		
        insert into SCHEMA_COMPARE_RESULTS VALUES (P_SOURCE_SCHEMA, P_TARGET_SCHEMA, r.TABLE_NAME, V_SOURCE_COUNT, V_TARGET_COUNT, -1, -1, V_SQLERRM);            
    end;               
                    
  end loop;
end;
$$ LANGUAGE plpgsql;
--
