use master
go
create or alter function fudgeCoordinate(@COORDINATE float, @SPATIAL_PRECISION int)
returns NUMERIC(28,18)
as
begin
  declare @DRIFT           float;
  declare @RESULT          NUMERIC(28,18)
  declare @FUDGE_FACTOR    float = 5e-12;
  
  if (@SPATIAL_PRECISION = 180) begin 
    if (@COORDINATE in (180.00000000000003)) begin
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
create or alter procedure sp_COMPARE_SCHEMA(@FORMAT_RESULTS bit,@SOURCE_DATABASE nvarchar(128), @SOURCE_SCHEMA nvarchar(128), @TARGET_DATABASE nvarchar(128), @TARGET_SCHEMA nvarchar(128), @COMMENT nvarchar(2048), @EMPTY_STRING_IS_NULL bit, @SPATIAL_PRECISION int, @DATE_TIME_PRECISION int) 
as
begin
  declare @OWNER            varchar(128);
  declare @TABLE_NAME       varchar(128);
  declare @COLUMN_LIST      nvarchar(max);
  declare @ALT_COLUMN_LIST  nvarchar(max);
  declare @SQL_STATEMENT    nvarchar(max);
  declare @BAD_STATEMENT    nvarchar(max);
  declare @C_NEWLINE        char(1) = char(10);
  
  declare @SOURCE_COUNT bigint;
  declare @TARGET_COUNT bigint;
  declare @MISSING_ROWS bigint;
  declare @EXTRA_ROWS   bigint;
  declare @SQLERRM      nvarchar(2000);
  
  declare FETCH_METADATA 
  cursor for 
  select t.TABLE_NAME
        ,string_agg(case 
                      when (c.DATA_TYPE in ('datetime2') and (c.DATETIME_PRECISION > @DATE_TIME_PRECISION)) then
                        -- concat('cast("',c.COLUMN_NAME,'" as datetime2(',@DATE_TIME_PRECISION,')) "',c.COLUMN_NAME,'"')
                        concat('convert(datetime2(',@DATE_TIME_PRECISION,'),convert(varchar(',@DATE_TIME_PRECISION+20,'),"',c.COLUMN_NAME,'"),126) "',c.COLUMN_NAME,'"')
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
                      when c.DATA_TYPE in ('geometry') then
                        -- concat('CONVERT(varchar(max), "',c.COLUMN_NAME,'".AsBinaryZM(),2) "',c.COLUMN_NAME,'"')
                        case 
                          when @SPATIAL_PRECISION = 18 then
                            concat('"',c.COLUMN_NAME,'".AsBinaryZM() "',c.COLUMN_NAME,'"')
                            -- concat('"',c.COLUMN_NAME,'".AsTextZM() "',c.COLUMN_NAME,'"')
                          else
                            concat('master.dbo.sp_geometryAsBinaryZM("',c.COLUMN_NAME,'",',@SPATIAL_PRECISION,') "',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('xml','text','ntext') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.COLUMN_NAME,'" as nvarchar(max))) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('image') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.COLUMN_NAME,'" as varbinary(max))) "',c.COLUMN_NAME,'"')
				      else  
                        concat('"',c.COLUMN_NAME,'"')
                      end
                   ,',') 
         within group (order by ordinal_position) "COLUMN_LIST"
        ,string_agg(case 
                      when (c.DATA_TYPE in ('datetime2') and (c.DATETIME_PRECISION > @DATE_TIME_PRECISION)) then
                        -- concat('cast("',c.COLUMN_NAME,'" as datetime2(',@DATE_TIME_PRECISION,')) "',c.COLUMN_NAME,'"')
                        concat('convert(datetime2(',@DATE_TIME_PRECISION,'),convert(varchar(',@DATE_TIME_PRECISION+20,'),"',c.COLUMN_NAME,'"),126) "',c.COLUMN_NAME,'"')
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
                        concat('master.dbo.sp_geographyAsBinaryZM("',c.COLUMN_NAME,'",180) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('geometry') then
                        concat('master.dbo.sp_geometryAsBinaryZM("',c.COLUMN_NAME,'",180) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('xml','text','ntext') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.COLUMN_NAME,'" as nvarchar(max))) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('image') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.COLUMN_NAME,'" as varbinary(max))) "',c.COLUMN_NAME,'"')
				      else  
                        concat('"',c.COLUMN_NAME,'"')
                      end
                   ,',') 
         within group (order by ordinal_position) "ALT_COLUMN_LIST"
   from INFORMATION_SCHEMA.COLUMNS c, INFORMATION_SCHEMA.TABLES t
  where t.TABLE_NAME = c.TABLE_NAME
    and t.TABLE_SCHEMA = c.TABLE_SCHEMA
    and t.TABLE_TYPE = 'BASE TABLE'
    and t.TABLE_SCHEMA = @SOURCE_SCHEMA
    -- and t.table_catalog = @SOURCE_DATABASE
  group by t.TABLE_SCHEMA, t.TABLE_NAME;
 
  set QUOTED_IDENTIFIER ON; 
  
  
  create table #SCHEMA_COMPARE_RESULTS (
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

  set NOCOUNT ON;

  open FETCH_METADATA;
  fetch FETCH_METADATA into @TABLE_NAME, @COLUMN_LIST, @ALT_COLUMN_LIST
  while @@FETCH_STATUS = 0 
  begin    
    begin try 
      set @SQL_STATEMENT = concat('select @SOURCE_COUNT = count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"')
      exec sp_executesql @SQL_STATEMENT,N'@SOURCE_COUNT bigint OUTPUT', @SOURCE_COUNT OUTPUT
          
      set @SQL_STATEMENT = concat('select @TARGET_COUNT = count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"')
      exec sp_executesql @SQL_STATEMENT,N'@TARGET_COUNT bigint OUTPUT', @TARGET_COUNT OUTPUT

      set @SQL_STATEMENT = concat('select @MISSING_ROWS = count(*) from (select ',@COLUMN_LIST,' from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT select ',@COLUMN_LIST,' from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'") T');
      exec sp_executesql @SQL_STATEMENT,N'@MISSING_ROWS bigint OUTPUT', @MISSING_ROWS OUTPUT

      set @SQL_STATEMENT = concat('select @EXTRA_ROWS = count(*) from (select ',@COLUMN_LIST,' from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT select ',@COLUMN_LIST,' from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'") T');
      exec sp_executesql @SQL_STATEMENT,N'@EXTRA_ROWS bigint OUTPUT', @EXTRA_ROWS OUTPUT
	  
	  if ((@MISSING_ROWS > 0) and (@EXTRA_ROWS > 0) and (@MISSING_ROWS = @EXTRA_ROWS)) begin
        set @SQL_STATEMENT = concat('select @MISSING_ROWS = count(*) from (select ',@ALT_COLUMN_LIST,' from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT select ',@ALT_COLUMN_LIST,' from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'") T');
        exec sp_executesql @SQL_STATEMENT,N'@MISSING_ROWS bigint OUTPUT', @MISSING_ROWS OUTPUT

        set @SQL_STATEMENT = concat('select @EXTRA_ROWS = count(*) from (select ',@ALT_COLUMN_LIST,' from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT select ',@ALT_COLUMN_LIST,' from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'") T');
        exec sp_executesql @SQL_STATEMENT,N'@EXTRA_ROWS bigint OUTPUT', @EXTRA_ROWS OUTPUT
	  end

      insert into #SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, @MISSING_ROWS, @EXTRA_ROWS, NULL, NULL)
    end try
    begin catch
	  select @SQL_STATEMENT
      if (ERROR_NUMBER() = 41317) 
      begin

        -- A user transaction that accesses memory optimized tables or natively compiled modules cannot access more than one user database or databases model and msdb, and it cannot write to master

        declare @SOURCE_HASH_BUCKET table(HASH varbinary(8000));
        declare @TARGET_HASH_BUCKET table(HASH varbinary(8000));

        begin try
                    
          set @SQL_STATEMENT = concat('WITH XML_TABLE(XML_DOC) as (select ',@COLUMN_LIST,' from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" for XML RAW, ELEMENTS XSINIL, BINARY BASE64, TYPE )',@C_NEWLINE,
                                      'select HASHBYTES(''SHA2_256'',cast(T2.ROW_XML.query(''.'') as nvarchar(max))) from XML_TABLE CROSS APPLY XML_DOC.nodes(''/Row'') as T2(ROW_XML)');
                                                                      
          insert into @SOURCE_HASH_BUCKET 
          exec(@SQL_STATEMENT)
       
          set @SQL_STATEMENT = concat('WITH XML_TABLE(XML_DOC) as (select ',@COLUMN_LIST,' from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" for XML RAW, ELEMENTS XSINIL, BINARY BASE64, TYPE )',@C_NEWLINE,
                                      'select HASHBYTES(''SHA2_256'',cast(T2.ROW_XML.query(''.'') as nvarchar(max))) from XML_TABLE CROSS APPLY XML_DOC.nodes(''/Row'') as T2(ROW_XML)');

          insert into @TARGET_HASH_BUCKET 
          exec(@SQL_STATEMENT)
          
          select @MISSING_ROWS = count(*) from (select HASH from @SOURCE_HASH_BUCKET EXCEPT select HASH from @TARGET_HASH_BUCKET) T1;
          select @EXTRA_ROWS = count(*) from (select HASH from @TARGET_HASH_BUCKET EXCEPT select HASH from @SOURCE_HASH_BUCKET) T1;

          insert into #SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, @MISSING_ROWS, @EXTRA_ROWS, NULL, NULL)
        end try
        begin catch
          set @BAD_STATEMENT = @SQL_STATEMENT
          set @SQLERRM = concat(ERROR_NUMBER(),': ',ERROR_MESSAGE())
        end catch
      end
      else 
      begin
        set @BAD_STATEMENT = @SQL_STATEMENT
        set @SQLERRM = concat(ERROR_NUMBER(),': ',ERROR_MESSAGE())
      end
    end catch
    
    if (@SQLERRM IS NOT NULL)
    begin
      begin try
        set @SQL_STATEMENT = concat('select @SOURCE_COUNT = count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"')
        exec sp_executesql @SQL_STATEMENT ,N'@SOURCE_COUNT bigint OUTPUT', @SOURCE_COUNT OUTPUT
      end try
      begin catch
         set @SQLERRM =  concat(ERROR_NUMBER(),': ',ERROR_MESSAGE(),'. ',@SQL_STATEMENT)
         set @SOURCE_COUNT = -1
      end catch
      begin try
        set @SQL_STATEMENT = concat('select @TARGET_COUNT = count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"')
        exec sp_executesql @SQL_STATEMENT,N'@TARGET_COUNT bigint OUTPUT', @TARGET_COUNT OUTPUT
      end try
      begin catch
        set @SQLERRM =  concat(ERROR_NUMBER(),': ',ERROR_MESSAGE(),'. ',@SQL_STATEMENT)
        set @TARGET_COUNT = -1
      end catch
      insert into #SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, -1, -1,@SQLERRM,@BAD_STATEMENT)
      set @SQLERRM = NULL
    end
    fetch FETCH_METADATA into @TABLE_NAME, @COLUMN_LIST, @ALT_COLUMN_LIST
  end
   
  close FETCH_METADATA;
  deallocate FETCH_METADATA;
  
  select @SOURCE_COUNT = COUNT(*) 
    from #SCHEMA_COMPARE_RESULTS
   where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
      or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
      or SQLERRM is not NULL
 
  if (@FORMAT_RESULTS = 1) 
  begin

    select cast(concat( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "',@SOURCE_DATABASE,'","',@TARGET_DATABASE,'", ',@COMMENT) as nvarchar(256)) " "
  
    set NOCOUNT OFF;

    select cast(FORMATMESSAGE('%32s %32s %48s %16s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, cast(TARGET_ROW_COUNT as varchar(16))) as nvarchar(256)) "SUCCESS          Source Schenma                    Target Schema                                            Table          Rows"
      from #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
--
    if (@SOURCE_COUNT > 0) 
    begin
      select cast(FORMATMESSAGE('%32s %32s %48s %16s %16s %16s %16s %64s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, cast(SOURCE_ROW_COUNT as varchar(16)), cast(TARGET_ROW_COUNT as varchar(16)), cast(MISSING_ROWS as varchar(16)), cast(EXTRA_ROWS as varchar(16)), SQLERRM) as nvarchar(256)) "FAILED           Source Schenma                    Target Schema                                            Table Details..."
        from #SCHEMA_COMPARE_RESULTS
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
      from #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
  
    select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM
      from #SCHEMA_COMPARE_RESULTS
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