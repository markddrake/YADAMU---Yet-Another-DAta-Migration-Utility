use master
go
create or alter function sp_showText(@INPUT NVARCHAR(MAX), @MAX_LINES BIGINT = 32)
returns @RESULTS table (
  LINE NVARCHAR(256)
) 
as
begin
  declare @LINE   BIGINT = 0
  declare @LENGTH BIGINT = len(@INPUT)
  while ((@LINE * 256 <= @LENGTH) and (@LINE <= @MAX_LINES)) begin
    insert into @RESULTS  values (substring(@INPUT,(@LINE*256)+1,256))
	set @LINE+=1;
 end
 return
end
go
--
execute sp_ms_marksystemobject 'sp_showText'
go
--
create or alter function fudgeCoordinate(@COORDINATE float, @SPATIAL_PRECISION int)
returns NUMERIC(28,18)
as
begin
  declare @DRIFT           float;
  declare @RESULT          NUMERIC(28,18)
  declare @FUDGE_FACTOR    float = 5e-12;
  
  if (@SPATIAL_PRECISION = -18) begin 
    if (@COORDINATE = 180.00000000000003) begin
	 -- Postgres Specific Fix.
      set @result = 180.0
	end
	else 
	begin
	  set @result = @COORDINATE;
	end
  end
  else 
  begin
    set @DRIFT = round((@COORDINATE-round(@COORDINATE,10,1))*1e10,0);
    set @RESULT = case
                    when (@DRIFT = -1) then
                      @COORDINATE - @FUDGE_FACTOR
                    when (@DRIFT = 1) then
                      @COORDINATE + @FUDGE_FACTOR
                    else
                      @COORDINATE			  
                    end;
     set @RESULT = round(round(@RESULT,13,0),10,1)
     set @RESULT = round(@RESULT,@SPATIAL_PRECISION,1);
   end;
   return @RESULT;
end
--
go
--
create or alter function sp_roundGeography(@geography geography,@SPATIAL_PRECISION int)
returns varchar(max)
as
begin
  declare @POINT_SEPERATOR   char(1);
  declare @POINT_NUMBER      int;
  declare @POINT_COUNT       int;
  
  declare @RING_SEPERATOR    char(1);
  declare @RING_NUMBER       int;
  declare @RING_COUNT        int;
  
  declare @POLYGON_SEPERATOR char(1);
  declare @POLYGON_NUMBER    int;
  declare @POLYGON_COUNT     int;

  declare @LAT               varchar(32);
  declare @LONG              varchar(32);
  declare @Z                 varchar(32);
  declare @M                 varchar(32);

  declare @RING              geography;
  declare @POINT             geography;
  declare @POLYGON           geography;
  declare @MULTIPOLYGON      geography;
  
  declare @WKT               varchar(max);

  if (@geography.STIsValid() = 0) begin
    set @geography = @geography.MakeValid();
  end;
  
  if @geography.InstanceOf('POINT') = 1 begin    
    set @LAT  = master.dbo.fudgeCoordinate(@geography.Lat,@SPATIAL_PRECISION)
    set @LONG = master.dbo.fudgeCoordinate(@geography.Long,@SPATIAL_PRECISION) 
    set @Z    = master.dbo.fudgeCoordinate(@geography.Z,@SPATIAL_PRECISION) 
    set @M    = master.dbo.fudgeCoordinate(@geography.M,@SPATIAL_PRECISION) 
    set @WKT = concat('POINT(',@LONG,' ',@LAT,' ',@Z,' ',@M,')')
    return @WKT   
  end;
 -- Iterative the component geograpjys and points for LINE, POLYGON, MULTIPOLYGON etc
  if @geography.InstanceOf('POLYGON') = 1 begin   
    set @WKT = 'POLYGON(';
    set @RING_NUMBER = 0;
    set @RING_SEPERATOR = ' ';
    set @RING_COUNT = @geography.NumRings();
    while (@RING_NUMBER  < @RING_COUNT) begin
      set @WKT = concat(@WKT,@RING_SEPERATOR,'(')
      set @RING_SEPERATOR = ',';
      set @RING_NUMBER = @RING_NUMBER + 1;
      set @RING = @geography.RingN(@RING_NUMBER);
      set @POINT_NUMBER = 0;
      set @POINT_SEPERATOR = ' ';
      set @POINT_COUNT = @RING.STNumPoints();
      while (@POINT_NUMBER < @POINT_COUNT) begin
        set @POINT_NUMBER = @POINT_NUMBER + 1;
        set @POINT = @RING.STPointN(@POINT_NUMBER);
        set @LAT  = master.dbo.fudgeCoordinate(@POINT.Lat,@SPATIAL_PRECISION)
        set @LONG = master.dbo.fudgeCoordinate(@POINT.Long,@SPATIAL_PRECISION) 
        set @Z    = master.dbo.fudgeCoordinate(@POINT.Z,@SPATIAL_PRECISION) 
        set @M    = master.dbo.fudgeCoordinate(@POINT.M,@SPATIAL_PRECISION) 
        set @WKT = concat(@WKT,@POINT_SEPERATOR,@LONG,' ',@LAT,' ',@Z,' ',@M)
        set @POINT_SEPERATOR = ',';
      end;
      set @WKT = concat(@WKT,')')      
    end
    set @WKT = concat(@WKT,')')   
    return @WKT   
  end;

  if @geography.InstanceOf('MULTIPOLYGON') = 1 begin   
    set @WKT = 'MULTIPOLYGON(';
    set @POLYGON_NUMBER = 0;
    set @POLYGON_SEPERATOR = ' ';
    set @POLYGON_COUNT = @geography.STNumGeometries();
    while (@POLYGON_NUMBER  < @POLYGON_COUNT) begin
      set @WKT = concat(@WKT,@POLYGON_SEPERATOR,'(')
      set @POLYGON_SEPERATOR = ',';
      set @POLYGON_NUMBER = @POLYGON_NUMBER + 1;
      set @POLYGON = @geography.STGeometryN(@POLYGON_NUMBER);
      set @RING_NUMBER = 0;
      set @RING_SEPERATOR = ' ';
      set @RING_COUNT = @POLYGON.NumRings();
      while (@RING_NUMBER  < @RING_COUNT) begin
        set @WKT = concat(@WKT,@RING_SEPERATOR,'(')
        set @RING_SEPERATOR = ',';
        set @RING_NUMBER = @RING_NUMBER + 1;
        set @RING = @POLYGON.RingN(@RING_NUMBER);
        set @POINT_NUMBER = 0;
        set @POINT_SEPERATOR = ' ';
        set @POINT_COUNT = @RING.STNumPoints();
        while (@POINT_NUMBER < @POINT_COUNT) begin
          set @POINT_NUMBER = @POINT_NUMBER + 1;
          set @POINT = @RING.STPointN(@POINT_NUMBER);
          set @LAT  = master.dbo.fudgeCoordinate(@POINT.Lat,@SPATIAL_PRECISION)
          set @LONG = master.dbo.fudgeCoordinate(@POINT.Long,@SPATIAL_PRECISION) 
          set @Z    = master.dbo.fudgeCoordinate(@POINT.Z,@SPATIAL_PRECISION) 
          set @M    = master.dbo.fudgeCoordinate(@POINT.M,@SPATIAL_PRECISION) 
          set @WKT = concat(@WKT,@POINT_SEPERATOR,@LONG,' ',@LAT,' ',@Z,' ',@M)
          set @POINT_SEPERATOR = ',';
        end;
        set @WKT = concat(@WKT,')')      
      end;
      set @WKT = concat(@WKT,')')      
    end;
    set @WKT = concat(@WKT,')')      
    return @WKT       
  end;     
  return @geography.AsTextZM();
end
--
go
execute sp_ms_marksystemobject 'sp_roundGeography'
go
--
create or alter function sp_geometryAsBinaryZM(@geometry geometry,@SPATIAL_PRECISION int)
returns varbinary(max)
as
begin
  declare @SPATIAL_LENGTH    int = @SPATIAL_PRECISION + 1;

  if @geometry.MakeValid().InstanceOf('POINT') = 1 
  begin
    declare @POINT      geometry
    declare @X          varchar(32);
    declare @Y          varchar(32);
    declare @Z          varchar(32);
    declare @M          varchar(32);
      
    set @X     = master.dbo.fudgeCoordinate(@geometry.STX,@SPATIAL_PRECISION)
    set @Y     = master.dbo.fudgeCoordinate(@geometry.STY,@SPATIAL_PRECISION) 
    set @Z     = master.dbo.fudgeCoordinate(@geometry.Z,@SPATIAL_PRECISION) 
    set @M     = master.dbo.fudgeCoordinate(@geometry.M,@SPATIAL_PRECISION) 
    set @POINT = geometry::STGeomFromText(concat('POINT(',@X,' ',@Y,' ',@Z,' ',@M,')'),0);      
    return @POINT.AsBinaryZM();
  end;
 -- Iterative the component geometry and points
  return @geometry.AsBinaryZM();
end
--
go
--
execute sp_ms_marksystemobject 'sp_geometryAsBinaryZM'
go
--
create or alter function sp_geographyAsTextZM(@geography geography,@SPATIAL_PRECISION int)
returns varchar(max)
as
begin
  return case
    when @geography is NULL then
      NULL
    else
      geography::STGeomFromText(master.dbo.sp_RoundGeography(@geography,@SPATIAL_PRECISION),@geography.STSrid).AsTextZM()
  end
end
--
go
--
execute sp_ms_marksystemobject 'sp_geographyAsTextZM'
go
--
create or alter function sp_geographyAsBinaryZM(@geography geography,@SPATIAL_PRECISION int)
returns varbinary(max)
as
begin
  return case
    when @geography is NULL then
      NULL
    else
      geography::STGeomFromText(master.dbo.sp_RoundGeography(@geography,@SPATIAL_PRECISION),@geography.STSrid).AsBinaryZM()
  end;
end
--
go
--
execute sp_ms_marksystemobject 'sp_geographyAsBinaryZM'
go
--
/*
**
** THere are Multiple ways of skinning the JSON_COMPACT cat...
**
*/
--
/*
create or alter function sp_jsonCompact(@JSON_INPUT NVARCHAR(MAX))
--
-- SPLIT_STRING, UPDATE: 00:05:05.274
--
returns NVARCHAR(MAX)
as
begin
  declare @TRUE                  BIT = 1;
  declare @FALSE                 BIT = 0;
  declare @JSON_OUTPUT NVARCHAR(MAX)
  
  declare @IN_STRING             BIT = @FALSE
  declare @FRAGMENT     NVARCHAR(MAX);
  declare @DUMMY        TABLE (
     DOC NVARCHAR(MAX)
  );
  
  DECLARE FIND_DOUBLE_QUOTES
  CURSOR FOR 
  select x.* 
    from string_split(@JSON_INPUT,'"') x;
    
  insert into @DUMMY values ('');
	
  open FIND_DOUBLE_QUOTES;
  fetch FIND_DOUBLE_QUOTES into @FRAGMENT
  while (@@FETCH_STATUS = 0) begin
    if (@IN_STRING = @FALSE) begin
      update @DUMMY set doc .write(replace(@FRAGMENT,' ',''),NULL,NULL)
	  set @IN_STRING = @TRUE
	end
	else begin
	  if (right(@FRAGMENT,1) = '\\') begin
	    update @DUMMY set doc .write(concat('"',@FRAGMENT),NULL,NULL)
      end
	  else begin
	    update @DUMMY set doc .write(concat('"',@FRAGMENT,'"'),NULL,NULL)
		set @IN_STRING = @FALSE
      end
	end
	fetch FIND_DOUBLE_QUOTES into @FRAGMENT
  end
  
  select @JSON_OUTPUT = DOC from @DUMMY;
  return @JSON_OUTPUT

end
go
*/
--
/*
create or alter function sp_jsonCompact(@JSON_INPUT NVARCHAR(MAX))
--
-- CHARINDEX LOOP, UPDATE DOC: 00:04:18.629
--
returns NVARCHAR(MAX)
as
begin
  declare @TRUE                  BIT = 1;
  declare @FALSE                 BIT = 0;

  declare @IN_STRING             BIT = @FALSE

  declare @JSON_OUTPUT TABLE (
     DOC  NVARCHAR(MAX)
  );
  
  declare @OFFSET                BIGINT = 1;
  declare @NEXT_QUOTE            BIGINT = CHARINDEX('"',@JSON_INPUT,@OFFSET);
  declare @FRAGMENT              NVARCHAR(MAX)
 
  insert into @JSON_OUTPUT values ('');
  while (@NEXT_QUOTE > 0) begin
    set @FRAGMENT = substring(@JSON_INPUT,@OFFSET,1+@NEXT_QUOTE-@OFFSET)
    if (@IN_STRING = @FALSE) begin
      set @FRAGMENT = replace(@FRAGMENT,' ','')
      set @IN_STRING = @TRUE
	end
	else begin
  	  if (right(@FRAGMENT,1) != '\\') begin
        set @IN_STRING = @FALSE
      end
	end
    update @JSON_OUTPUT set DOC .write(@FRAGMENT,NULL,NULL)
    set @OFFSET = @NEXT_QUOTE + 1
	set @NEXT_QUOTE = CHARINDEX('"',@JSON_INPUT,@OFFSET);
	
  end
  
  set @FRAGMENT = substring(@JSON_INPUT,@OFFSET,LEN(@JSON_INPUT))
  set @FRAGMENT = replace(@FRAGMENT,' ','')
  update @JSON_OUTPUT set DOC .write(@FRAGMENT,NULL,NULL)  

  select @FRAGMENT = DOC from @JSON_OUTPUT;
  return @FRAGMENT

end
go
*/
--
/*
create or alter function sp_jsonCompact(@INPUT_JSON NVARCHAR(MAX))
--
-- CHAR BY CHAR: STUFF : 00:04:22.742
--
returns NVARCHAR(MAX)
as
begin
  declare @TRUE                  BIT = 1;
  declare @FALSE                 BIT = 0;
  declare @INPUT_LENGTH       BIGINT = len(@INPUT_JSON);
  declare @OUTPUT_JSON NVARCHAR(MAX) = replicate(' ',@INPUT_LENGTH);
  declare @I_OFFSET           BIGINT = 1
  declare @O_OFFSET           BIGINT = 1
  declare @NEXT_CHAR     NVARCHAR(1)
  declare @prev_char     NVARCHAR(1) = ''
  declare @IN_STRING             BIT = @FALSE
  while @INPUT_LENGTH >= @I_OFFSET begin
    set @NEXT_CHAR = substring(@INPUT_JSON,@I_OFFSET,1);
	set @I_OFFSET += 1;
    if ((@IN_STRING = @TRUE) OR (@NEXT_CHAR <> ' ')) begin
      set @OUTPUT_JSON = stuff(@OUTPUT_JSON,@O_OFFSET,1,@NEXT_CHAR)
	  set @O_OFFSET += 1
	end
	if (@NEXT_CHAR = '"') begin
	  if (@IN_STRING = @TRUE) begin
   	    set @IN_STRING = case 
		                   when ((@IN_STRING = @TRUE) and (@PREV_CHAR = '\\')) then 
						     @TRUE
		                   else 
						     @FALSE
	                     end
      end 						 
	  else begin
	    set @IN_STRING = @TRUE
      end
	end
	set @PREV_CHAR = @NEXT_CHAR
  end
  return rtrim(@OUTPUT_JSON)
end
go
*/
--
create or alter function sp_jsonCompact(@JSON_INPUT NVARCHAR(MAX))
returns NVARCHAR(MAX)
--
-- CHARINDEX LOOP, STUFF: 00:00:25.192
--
as
begin
  declare @TRUE                  BIT = 1;
  declare @FALSE                 BIT = 0;

  declare @IN_STRING             BIT = @FALSE

  declare @INPUT_LENGTH          BIGINT = len(@JSON_INPUT);
  declare @JSON_OUTPUT           NVARCHAR(MAX) = replicate(' ',@INPUT_LENGTH);
    
  declare @OFFSET                BIGINT = 1;
  declare @NEXT_QUOTE            BIGINT = CHARINDEX('"',@JSON_INPUT,@OFFSET);
  declare @LAST_QUOTE            BIGINT = 1;
  
  declare @FRAGMENT              NVARCHAR(MAX)
  declare @FRAGMENT_LENGTH       BIGINT;
 
  while (@NEXT_QUOTE > 0) begin
    set @FRAGMENT_LENGTH = 1+@NEXT_QUOTE-@LAST_QUOTE
    set @FRAGMENT = substring(@JSON_INPUT,@LAST_QUOTE,@FRAGMENT_LENGTH)
    if (@IN_STRING = @FALSE) begin
      set @FRAGMENT = replace(@FRAGMENT,' ','')
	  set @FRAGMENT_LENGTH = len(@FRAGMENT);
      set @IN_STRING = @TRUE
	end
	else begin
  	  if (right(@FRAGMENT,2) != '\"') begin
        set @IN_STRING = @FALSE
      end
	end

    set @JSON_OUTPUT = stuff(@JSON_OUTPUT,@OFFSET,@FRAGMENT_LENGTH,@FRAGMENT)
	set @OFFSET += @FRAGMENT_LENGTH
	set @LAST_QUOTE = @NEXT_QUOTE+1;
    set @NEXT_QUOTE = CHARINDEX('"',@JSON_INPUT,@LAST_QUOTE);
	
  end
  
  set @FRAGMENT = substring(@JSON_INPUT,@LAST_QUOTE,LEN(@JSON_INPUT))
  set @FRAGMENT = replace(@FRAGMENT,' ','')
  set @FRAGMENT_LENGTH = len(@FRAGMENT);
  set @JSON_OUTPUT = stuff(@JSON_OUTPUT,@OFFSET,@FRAGMENT_LENGTH,@FRAGMENT)

  return rtrim(@JSON_OUTPUT)

end
go
--
execute sp_ms_marksystemobject 'sp_jsonCompact'
go
--
create or alter function sp_jsonOrder(@JSON nvarchar(max))
returns nvarchar(max)
as
begin
  return (
     select '{' + (STRING_AGG('"' + "key" + ':' + case when ISJSON(value) = 1 then dbo.sp_jsonOrder(value) else '"' + value + '"' end,',') WITHIN GROUP (ORDER BY "key")) + '}' from OPENJSON(@JSON)
  )
end
go
--
execute sp_ms_marksystemobject 'sp_jsonOrder'
go
--
create or alter function sp_xmlNormalize(@XML_RULE nvarchar(128), @XML_VALUE xml)
returns nvarchar(max) 
as
begin
  declare @XML_DECLARATION  nvarchar(21) = '<?xml version="1.0"?>';
  declare @RESULT           nvarchar(max);
  set @RESULT = convert(nvarchar(max),@XML_VALUE,1);
  if (@XML_RULE = 'DECODE_AND_STRIP_DECLARATION') begin
	if (left(@RESULT,len(@XML_DECLARATION)) = @XML_DECLARATION) begin
	  set @RESULT = STUFF(@RESULT,1,len(@XML_DECLARATION),'');
    end
  end
  if (@XML_RULE = 'TRIM_WHITESPACE') begin
    set @RESULT = TRIM(CHAR(13)+CHAR(10) from @RESULT)
  end
  return @RESULT;
end
go
--
execute sp_ms_marksystemobject 'sp_xmlNormalize'
go
--
create or alter procedure sp_COMPARE_SCHEMA(@FORMAT_RESULTS bit,@SOURCE_DATABASE nvarchar(128), @SOURCE_SCHEMA nvarchar(128), @TARGET_DATABASE nvarchar(128), @TARGET_SCHEMA nvarchar(128), @COMMENT nvarchar(2048), @RULES NVARCHAR(MAX)) 
as
begin
  declare @OWNER            varchar(128);
  declare @TABLE_NAME       varchar(128);
  declare @MEMORY_OPTIMIZED bit;
  declare @COLUMN_LIST      nvarchar(max);
  declare @SQL_STATEMENT    nvarchar(max);
  declare @BAD_STATEMENT    nvarchar(max);
  declare @C_NEWLINE        char(1) = char(10);
  
  declare @SOURCE_COUNT bigint;
  declare @TARGET_COUNT bigint;
  declare @MISSING_ROWS bigint;
  declare @EXTRA_ROWS   bigint;
  declare @SQLERRM      nvarchar(2000);

  declare @EMPTY_STRING_IS_NULL BIT           = case when JSON_VALUE(@RULES,'$.emptyStringIsNull') = 'true' then 1 else 0 end;
  declare @MIN_BIGINT_IS_NULL   BIT           = case when JSON_VALUE(@RULES,'$.minBigIntIsNull') = 'true' then 1 else 0 end;
  declare @TIMESTAMP_PRECISION  INT           = JSON_VALUE(@RULES,'$.timestampPrecision'); 
                                                                   
  declare @SPATIAL_PRECISION    INT           = JSON_VALUE(@RULES,'$.spatialPrecision');
  declare @DOUBLE_PRECISION     INT           = JSON_VALUE(@RULES,'$.doublePrecision');
  declare @ORDRED_JSON          BIT           = case when JSON_VALUE(@RULES,'$.orderedJSON') = 'true' then 1 else 0 end;;
  declare @XML_RULE             NVARCHAR(128) = JSON_VALUE(@RULES,'$.xmlRule');
   
  -- SSQL Server shows the wrong tables as memory optimized. If you try to filter on sys.tables.isMemoryOptimized you get wrong results.... 
   
  declare FETCH_METADATA 
  cursor for 
  select t.TABLE_NAME
        ,string_agg(case 
	                  when cc."CONSTRAINT_NAME" is not NULL then 
					     concat('master.dbo.sp_jsonOrder("',c.COLUMN_NAME,'") "',c.COLUMN_NAME,'"')
                      when (c.DATA_TYPE in ('datetime2') and (c.DATETIME_PRECISION > @TIMESTAMP_PRECISION)) then
                       -- concat('cast("',c.COLUMN_NAME,'" as datetime2(',@TIMESTAMP_PRECISION,')) "',c.COLUMN_NAME,'"')
                        concat('convert(datetime2(',@TIMESTAMP_PRECISION,'),convert(varchar(',@TIMESTAMP_PRECISION+20,'),"',c.COLUMN_NAME,'"),126) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('bigint') then
                        case 
                          when @MIN_BIGINT_IS_NULL = 1 then
                            concat('case when "',c.COLUMN_NAME,'" = -9223372036854775808 then NULL else "',c.COLUMN_NAME,'" end "',c.COLUMN_NAME,'"')
                          else
                            concat('"',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('varchar','nvarchar') then
                        case 
                          when @EMPTY_STRING_IS_NULL = 1 then
                            concat('case when "',c.COLUMN_NAME,'" = '''' then NULL else "',c.COLUMN_NAME,'" end collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                          else
                            concat('"',c.COLUMN_NAME,'" collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('char','nchar') then
                        concat('"',c.COLUMN_NAME,'" collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')					  
                      when c.DATA_TYPE in ('geography') then
                        case 
                          when @SPATIAL_PRECISION = 18 then
                            concat('"',c.COLUMN_NAME,'".AsBinaryZM() "',c.COLUMN_NAME,'"')
                           -- concat('"',c.COLUMN_NAME,'".AsTextZM() "',c.COLUMN_NAME,'"')
                          else
                            concat('master.dbo.sp_geographyAsBinaryZM("',c.COLUMN_NAME,'",',@SPATIAL_PRECISION,') "',c.COLUMN_NAME,'"')
                        end
				      when c.DATA_TYPE in ('float','real') and (@DOUBLE_PRECISION < 18) then
					     concat('round("',c.COLUMN_NAME,'",',@DOUBLE_PRECISION,') "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('geometry') then
                       -- concat('CONVERT(varchar(max), "',c.COLUMN_NAME,'".AsBinaryZM(),2) "',c.COLUMN_NAME,'"')
                        case 
                          when @SPATIAL_PRECISION = 18 then
                            concat('"',c.COLUMN_NAME,'".AsBinaryZM() "',c.COLUMN_NAME,'"')
                            -- concat('"',c.COLUMN_NAME,'".AsTextZM() "',c.COLUMN_NAME,'"')
                          else
                            concat('master.dbo.sp_geometryAsBinaryZM("',c.COLUMN_NAME,'",',@SPATIAL_PRECISION,') "',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('xml') then
                        concat('master.dbo.sp_xmlNormalize(''',@XML_RULE,''',"',c.COLUMN_NAME,'")  collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('xml','text','ntext') then
                        concat('CAST("',c.COLUMN_NAME,'" as nvarchar(max))  collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('image') then
                        concat('CAST("',c.COLUMN_NAME,'" as varbinary(max)) "',c.COLUMN_NAME,'"')
				      else  
                        concat('"',c.COLUMN_NAME,'"')
                      end
                   ,',') 
         within group (order by ordinal_position) "COLUMN_LIST"
   from "INFORMATION_SCHEMA"."COLUMNS" c
        left join "INFORMATION_SCHEMA"."TABLES" t
               on t."TABLE_CATALOG" = c."TABLE_CATALOG"
              and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
              and t."TABLE_NAME" = c."TABLE_NAME"
        left outer join (
                          "INFORMATION_SCHEMA"."CONSTRAINT_COLUMN_USAGE" ccu
                          left join "INFORMATION_SCHEMA"."CHECK_CONSTRAINTS" cc
                              on cc."CONSTRAINT_CATALOG" = ccu."CONSTRAINT_CATALOG"
                             and cc."CONSTRAINT_SCHEMA" = ccu."CONSTRAINT_SCHEMA"
                             and cc."CONSTRAINT_NAME" = ccu."CONSTRAINT_NAME"
                        )
                     on ccu."TABLE_CATALOG" = c."TABLE_CATALOG"
                    and ccu."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                    and ccu."TABLE_NAME" = c."TABLE_NAME"
                    and ccu."COLUMN_NAME" = c."COLUMN_NAME"
                    and UPPER("CHECK_CLAUSE") like '(ISJSON(%)>(0))'
  where t."TABLE_TYPE" = 'BASE TABLE'
    and t."TABLE_SCHEMA" = @SOURCE_SCHEMA
  group by t.TABLE_SCHEMA, t.TABLE_NAME
 
  set QUOTED_IDENTIFIER ON; 
  
  declare @SCHEMA_COMPARE_RESULTS TABLE(
    SOURCE_DATABASE  nvarchar(128)
   ,SOURCE_SCHEMA    nvarchar(128)
   ,TARGET_DATABASE  nvarchar(128)
   ,TARGET_SCHEMA    nvarchar(128)
   ,TABLE_NAME       nvarchar(128)
   ,SOURCE_ROW_COUNT bigint
   ,TARGET_ROW_COUNT bigint
   ,MISSING_ROWS     bigint
   ,EXTRA_ROWS       bigint
   ,SQLERRM          nvarchar(2048)
   ,SQL_STATEMENT    nvarchar(max)
  );

  CREATE TABLE #SOURCE_HASH_BUCKET (
    HASH BINARY(32)
  )

  CREATE TABLE #TARGET_HASH_BUCKET (
    HASH BINARY(32)
  )

  set NOCOUNT ON;

  open FETCH_METADATA;
  fetch FETCH_METADATA into @TABLE_NAME, @COLUMN_LIST
  while @@FETCH_STATUS = 0 
  begin    
    begin try 
      set @SOURCE_COUNT = -1;
      set @SQL_STATEMENT = concat('select @SOURCE_COUNT = count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"')
	 -- select @SQL_STATEMENT
      
	  exec sp_executesql @SQL_STATEMENT,N'@SOURCE_COUNT bigint OUTPUT', @SOURCE_COUNT OUTPUT
        
      set @TARGET_COUNT = -1;
      set @SQL_STATEMENT = concat('select @TARGET_COUNT = count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"')
	 -- select @SQL_STATEMENT
	  
      exec sp_executesql @SQL_STATEMENT,N'@TARGET_COUNT bigint OUTPUT', @TARGET_COUNT OUTPUT
      
      set @SQL_STATEMENT = concat('with ',
                                  'SOURCE_ROWS as (',
                                  '  select ',@COLUMN_LIST,' from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"',
                                  '),',
                                  'TARGET_ROWS as (',
                                  '  select ',@COLUMN_LIST,' from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"',
                                  '),',
                                   'MISSING_ROWS as (',
                                  ' select * from SOURCE_ROWS EXCEPT select * from TARGET_ROWS ',
                                  '),',
                                  'EXTRA_ROWS as (',
                                  ' select * from TARGET_ROWS EXCEPT select * from SOURCE_ROWS ',
                                  ')',
                                  'select ''',@SOURCE_DATABASE,''' "SOURCE_DATABASE",''',@SOURCE_SCHEMA,''' "SOURCE_SCHEMA",''',@TARGET_DATABASE,'''"TARGET_DATABASE",''',@TARGET_SCHEMA,'''"TARGET_SCHEMA",''',@TABLE_NAME,'''"TABLE_NAME",',
                                              @SOURCE_COUNT,' "SOURCE_ROWS", ',@TARGET_COUNT,' "TARGET_ROWS", (select count(*) from MISSING_ROWS) "MISSING_ROWS",(select count(*) from EXTRA_ROWS) "EXTRA_ROWS", NULL "SQLERRM", NULL "SQL_STATEMENT" ')   
      --select @SQL_STATEMENT;

      insert into @SCHEMA_COMPARE_RESULTS                  
      exec (@SQL_STATEMENT)
    end try
    begin catch
     -- Encountered a memory optizmed tables..
      if (ERROR_NUMBER() = 41317) begin
        begin try 
         -- A user transaction that accesses memory optimized tables or natively compiled modules cannot access more than one user database or databases model and msdb, and it cannot write to master
    	  truncate table #SOURCE_HASH_BUCKET;
		  truncate table #TARGET_HASH_BUCKET;
        
		  set @SQL_STATEMENT = concat('select HASHBYTES(''SHA2_256'',cast((select ',@COLUMN_LIST,' for JSON PATH, INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER ) as nvarchar(max))) HASH from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"');
         -- select @SQL_STATEMENT
          
		  insert into #SOURCE_HASH_BUCKET
          exec(@SQL_STATEMENT);

 		  set @SQL_STATEMENT = concat('select HASHBYTES(''SHA2_256'',cast((select ',@COLUMN_LIST,' for JSON PATH, INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER ) as nvarchar(max))) HASH from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"');
         -- select @SQL_STATEMENT
        
		  insert into #TARGET_HASH_BUCKET
          exec(@SQL_STATEMENT);
        
		  with
          MISSING_ROWS as (
            select * from #SOURCE_HASH_BUCKET EXCEPT select * from #TARGET_HASH_BUCKET
          ),
          EXTRA_ROWS as (
            select * from #TARGET_HASH_BUCKET EXCEPT select * from #SOURCE_HASH_BUCKET
          )
		  insert into @SCHEMA_COMPARE_RESULTS                  
          select @SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@TABLE_NAME, @SOURCE_COUNT,@TARGET_COUNT,(select count(*) from MISSING_ROWS),(select count(*) from EXTRA_ROWS), NULL, NULL;
		
		end try
        begin catch 
          set @BAD_STATEMENT = @SQL_STATEMENT
          set @SQLERRM = concat(ERROR_NUMBER(),': ',ERROR_MESSAGE())
          insert into @SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, -1, -1, @SQLERRM , @BAD_STATEMENT)
         set @SQLERRM = NULL
        end catch
      end
      else begin      
        set @BAD_STATEMENT = @SQL_STATEMENT
        set @SQLERRM = concat(ERROR_NUMBER(),': ',ERROR_MESSAGE())
        insert into @SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, -1, -1, @SQLERRM , @BAD_STATEMENT)
        set @SQLERRM = NULL
      end
    end catch
	--
	-- Probably Overkill: Workaround for 
	--
	-- 2019-10-14 06:17:11.47 spid22s     AppDomain 10 (master.sys[runtime].9) is marked for unload due to memory pressure.
    -- 2019-10-14 06:17:12.42 spid22s     AppDomain 10 (master.sys[runtime].9) unloaded.
   --
	-- When comparing rows containing complex spatial data.
	--
	
   -- DBCC FREESYSTEMCACHE ('ALL') WITH NO_INFOMSGS
	-- DBCC FREESESSIONCACHE WITH NO_INFOMSGS
	-- DBCC FREEPROCCACHE WITH NO_INFOMSGS
	
    fetch FETCH_METADATA into @TABLE_NAME, @COLUMN_LIST
  end
   
  close FETCH_METADATA;
  deallocate FETCH_METADATA;
  
  select @SOURCE_COUNT = COUNT(*) 
    from @SCHEMA_COMPARE_RESULTS
   where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
      or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
      or SQLERRM is not NULL
 
  if (@FORMAT_RESULTS = 1) 
  begin

    select cast(concat( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "',@SOURCE_DATABASE,'","',@TARGET_DATABASE,'", ',@COMMENT) as nvarchar(256)) " "
  
    set NOCOUNT OFF;

    select cast(FORMATMESSAGE('%32s %32s %48s %16s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, cast(TARGET_ROW_COUNT as varchar(16))) as nvarchar(256)) "SUCCESS          Source Schenma                    Target Schema                                            Table          Rows"
      from @SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
--
    if (@SOURCE_COUNT > 0) 
    begin
      select cast(FORMATMESSAGE('%32s %32s %48s %16s %16s %16s %16s %64s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, cast(SOURCE_ROW_COUNT as varchar(16)), cast(TARGET_ROW_COUNT as varchar(16)), cast(MISSING_ROWS as varchar(16)), cast(EXTRA_ROWS as varchar(16)), SQLERRM) as nvarchar(256)) "FAILED           Source Schenma                    Target Schema                                            Table Details..."
        from @SCHEMA_COMPARE_RESULTS
       where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
          or MISSING_ROWS <> 0
         or EXTRA_ROWS <> 0
         or SQLERRM is not NULL
      order by TABLE_NAME;
    end;
  end
  else 
  begin
    select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
      from @SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
  
    select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM, SQL_STATEMENT
      from @SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
        or MISSING_ROWS <> 0
        or EXTRA_ROWS <> 0
        or SQLERRM is not NULL
     order by TABLE_NAME;
  end  
--
end
--
go
--
execute sp_ms_marksystemobject 'sp_COMPARE_SCHEMA'
go
--
exit