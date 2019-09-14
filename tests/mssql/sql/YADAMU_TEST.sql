use master
go
CREATE OR ALTER FUNCTION pointsFromGeography(@GEOGRAPHY GEOGRAPHY)
returns @POINTS_TABLE TABLE (
    ID  INT,
    LAT FLOAT,
    LONG FLOAT
  )
as
BEGIN

  DECLARE @POINT_NUMBER      INT;
  DECLARE @POINT_COUNT       INT;
  
  DECLARE @RING_NUMBER       INT;
  DECLARE @RING_COUNT        INT;
  
  DECLARE @POLYGON_NUMBER    INT;
  DECLARE @POLYGON_COUNT     INT;

  DECLARE @RING              GEOGRAPHY;
  DECLARE @POINT             GEOGRAPHY;
  DECLARE @POLYGON           GEOGRAPHY;
  DECLARE @MULTIPOLYGON      GEOGRAPHY;
  DECLARE @POINT_ID          INT = 1;
  
  if (@GEOGRAPHY.STIsValid() = 0) begin
    SET @GEOGRAPHY = @GEOGRAPHY.MakeValid();
  end;
  
  if @GEOGRAPHY.InstanceOf('POINT') = 1 begin    
    insert into @POINTS_TABLE (ID, LAT, LONG) values (1,@GEOGRAPHY.Lat,@GEOGRAPHY.Long)
  end;
  
  -- Iterative the component geograpjys and points for LINE, POLYGON, MULTIPOLYGON etc
  if @GEOGRAPHY.InstanceOf('POLYGON') = 1 begin   
    SET @RING_NUMBER = 0;
    SET @RING_COUNT = @GEOGRAPHY.NumRings();
    while (@RING_NUMBER  < @RING_COUNT) begin
      SET @RING_NUMBER = @RING_NUMBER + 1;
      SET @RING = @GEOGRAPHY.RingN(@RING_NUMBER);
      SET @POINT_NUMBER = 0;
      SET @POINT_COUNT = @RING.STNumPoints();
      while (@POINT_NUMBER < @POINT_COUNT) begin
        SET @POINT_NUMBER = @POINT_NUMBER + 1;
        SET @POINT = @RING.STPointN(@POINT_NUMBER);
        insert into @POINTS_TABLE (ID, LAT, LONG) values (@POINT_ID,@POINT.Lat,@POINT.Long)
        SET @POINT_ID = @POINT_ID + 1;
      end;
    end
  end;

  if @GEOGRAPHY.InstanceOf('MULTIPOLYGON') = 1 begin   
    SET @POLYGON_NUMBER = 0;
    SET @POLYGON_COUNT = @GEOGRAPHY.STNumGeometries();
    while (@POLYGON_NUMBER  < @POLYGON_COUNT) begin
      SET @POLYGON_NUMBER = @POLYGON_NUMBER + 1;
      SET @POLYGON = @GEOGRAPHY.STGeometryN(@POLYGON_NUMBER);
      SET @RING_NUMBER = 0;
      SET @RING_COUNT = @POLYGON.NumRings();
      while (@RING_NUMBER  < @RING_COUNT) begin
        SET @RING_NUMBER = @RING_NUMBER + 1;
        SET @RING = @POLYGON.RingN(@RING_NUMBER);
        SET @POINT_NUMBER = 0;
        SET @POINT_COUNT = @RING.STNumPoints();
        while (@POINT_NUMBER < @POINT_COUNT) begin
          SET @POINT_NUMBER = @POINT_NUMBER + 1;
          SET @POINT = @RING.STPointN(@POINT_NUMBER);
          insert into @POINTS_TABLE (ID, LAT, LONG) values (@POINT_ID,@POINT.Lat,@POINT.Long)
          SET @POINT_ID = @POINT_ID + 1;
        end;
      end;
    end;
  end;     
  return 
end
--
go
--
CREATE OR ALTER FUNCTION sp_RoundGeography(@GEOGRAPHY GEOGRAPHY,@SPATIAL_PRECISION int)
returns VARCHAR(MAX)
as
begin
  DECLARE @POINT_SEPERATOR   CHAR(1);
  DECLARE @POINT_NUMBER      INT;
  DECLARE @POINT_COUNT       INT;
  
  DECLARE @RING_SEPERATOR    CHAR(1);
  DECLARE @RING_NUMBER       INT;
  DECLARE @RING_COUNT        INT;
  
  DECLARE @POLYGON_SEPERATOR CHAR(1);
  DECLARE @POLYGON_NUMBER    INT;
  DECLARE @POLYGON_COUNT     INT;

  DECLARE @LAT               VARCHAR(32);
  DECLARE @LONG              VARCHAR(32);
  DECLARE @Z                 VARCHAR(32);
  DECLARE @M                 VARCHAR(32);

  DECLARE @RING              GEOGRAPHY;
  DECLARE @POINT             GEOGRAPHY;
  DECLARE @POLYGON           GEOGRAPHY;
  DECLARE @MULTIPOLYGON      GEOGRAPHY;
  
  DECLARE @SPATIAL_LENGTH    INT = @SPATIAL_PRECISION + 1;
  DECLARE @WKT               VARCHAR(MAX);

  if (@GEOGRAPHY.STIsValid() = 0) begin
    SET @GEOGRAPHY = @GEOGRAPHY.MakeValid();
  end;
  
  if @GEOGRAPHY.InstanceOf('POINT') = 1 begin    
    -- SET @LAT = STR(ROUND(@GEOGRAPHY.Lat,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    -- SET @LONG = STR(ROUND(@GEOGRAPHY.Long,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    -- SET @Z = STR(ROUND(@GEOGRAPHY.Z,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    -- SET @M = STR(ROUND(@GEOGRAPHY.M,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    SET @LAT  = round(round(cast(@GEOGRAPHY.Lat as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1)
    SET @LONG = round(round(cast(@GEOGRAPHY.Long as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
    SET @Z    = round(round(cast(@GEOGRAPHY.Z as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
    SET @M    = round(round(cast(@GEOGRAPHY.M as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
    SET @WKT = concat('POINT(',@LONG,' ',@LAT,' ',@Z,' ',@M,')')
    return @WKT   
  end;
  -- Iterative the component geograpjys and points for LINE, POLYGON, MULTIPOLYGON etc
  if @GEOGRAPHY.InstanceOf('POLYGON') = 1 begin   
    SET @WKT = 'POLYGON(';
    SET @RING_NUMBER = 0;
    SET @RING_SEPERATOR = ' ';
    SET @RING_COUNT = @GEOGRAPHY.NumRings();
    while (@RING_NUMBER  < @RING_COUNT) begin
      SET @WKT = concat(@WKT,@RING_SEPERATOR,'(')
      SET @RING_SEPERATOR = ',';
      SET @RING_NUMBER = @RING_NUMBER + 1;
      SET @RING = @GEOGRAPHY.RingN(@RING_NUMBER);
      SET @POINT_NUMBER = 0;
      SET @POINT_SEPERATOR = ' ';
      SET @POINT_COUNT = @RING.STNumPoints();
      while (@POINT_NUMBER < @POINT_COUNT) begin
        SET @POINT_NUMBER = @POINT_NUMBER + 1;
        SET @POINT = @RING.STPointN(@POINT_NUMBER);
        -- SET @LAT = STR(ROUND(@POINT.Lat,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
        -- SET @LONG = STR(ROUND(@POINT.Long,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
        -- SET @Z = STR(ROUND(@POINT.Z,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
        -- SET @M = STR(ROUND(@POINT.M,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
        SET @LAT  = round(round(cast(@POINT.Lat as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1)
        SET @LONG = round(round(cast(@POINT.Long as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
        SET @Z    = round(round(cast(@POINT.Z as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
        SET @M    = round(round(cast(@POINT.M as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
        SET @WKT = concat(@WKT,@POINT_SEPERATOR,@LONG,' ',@LAT,' ',@Z,' ',@M)
        SET @POINT_SEPERATOR = ',';
      end;
      SET @WKT = concat(@WKT,')')      
    end
    SET @WKT = concat(@WKT,')')   
    return @WKT   
  end;

  if @GEOGRAPHY.InstanceOf('MULTIPOLYGON') = 1 begin   
    SET @WKT = 'MULTIPOLYGON(';
    SET @POLYGON_NUMBER = 0;
    SET @POLYGON_SEPERATOR = ' ';
    SET @POLYGON_COUNT = @GEOGRAPHY.STNumGeometries();
    while (@POLYGON_NUMBER  < @POLYGON_COUNT) begin
      SET @WKT = concat(@WKT,@POLYGON_SEPERATOR,'(')
      SET @POLYGON_SEPERATOR = ',';
      SET @POLYGON_NUMBER = @POLYGON_NUMBER + 1;
      SET @POLYGON = @GEOGRAPHY.STGeometryN(@POLYGON_NUMBER);
      SET @RING_NUMBER = 0;
      SET @RING_SEPERATOR = ' ';
      SET @RING_COUNT = @POLYGON.NumRings();
      while (@RING_NUMBER  < @RING_COUNT) begin
        SET @WKT = concat(@WKT,@RING_SEPERATOR,'(')
        SET @RING_SEPERATOR = ',';
        SET @RING_NUMBER = @RING_NUMBER + 1;
        SET @RING = @POLYGON.RingN(@RING_NUMBER);
        SET @POINT_NUMBER = 0;
        SET @POINT_SEPERATOR = ' ';
        SET @POINT_COUNT = @RING.STNumPoints();
        while (@POINT_NUMBER < @POINT_COUNT) begin
          SET @POINT_NUMBER = @POINT_NUMBER + 1;
          SET @POINT = @RING.STPointN(@POINT_NUMBER);
          -- SET @LAT = STR(ROUND(@POINT.Lat,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
          -- SET @LONG = STR(ROUND(@POINT.Long,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
          -- SET @Z = STR(ROUND(@POINT.Z,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
          -- SET @M = STR(ROUND(@POINT.M,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
          SET @LAT  = round(round(cast(@POINT.Lat as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1)
          SET @LONG = round(round(cast(@POINT.Long as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
          SET @Z    = round(round(cast(@POINT.Z as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
          SET @M    = round(round(cast(@POINT.M as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
          SET @WKT = concat(@WKT,@POINT_SEPERATOR,@LONG,' ',@LAT,' ',@Z,' ',@M)
          SET @POINT_SEPERATOR = ',';
        end;
        SET @WKT = concat(@WKT,')')      
      end;
      SET @WKT = concat(@WKT,')')      
    end;
    SET @WKT = concat(@WKT,')')      
    return @WKT       
  end;     
  return @GEOGRAPHY.AsTextZM();
end
--
GO
EXECUTE sp_ms_marksystemobject 'sp_TRUNCGeography'
GO
--
CREATE OR ALTER FUNCTION sp_geometryAsBinaryZM(@GEOMETRY GEOMETRY,@SPATIAL_PRECISION int)
returns VARBINARY(MAX)
as
begin
  DECLARE @SPATIAL_LENGTH    INT = @SPATIAL_PRECISION + 1;

  if @GEOMETRY.MakeValid().InstanceOf('POINT') = 1 
  begin
    DECLARE @POINT      GEOMETRY
    DECLARE @X          VARCHAR(32);
    DECLARE @Y          VARCHAR(32);
    DECLARE @Z          VARCHAR(32);
    DECLARE @M          VARCHAR(32);
      
    -- SET @X = STR(ROUND(@GEOMETRY.STX,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    -- SET @Y = STR(ROUND(@GEOMETRY.STY,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    -- SET @Z = STR(ROUND(@GEOMETRY.Z,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    -- SET @M = STR(ROUND(@GEOMETRY.M,@SPATIAL_TRUNC,1),@SPATIAL_LENGTH,@SPATIAL_PRECISION);
    SET @X    = round(round(cast(@GEOMETRY.STX as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1)
    SET @Y    = round(round(cast(@GEOMETRY.STY as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
    SET @Z    = round(round(cast(@GEOMETRY.Z as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
    SET @M    = round(round(cast(@GEOMETRY.M as NUMERIC(18,12)),@SPATIAL_LENGTH,0),@SPATIAL_PRECISION,1) 
    SET @POINT = geometry::STGeomFromText(concat('POINT(',@X,' ',@Y,' ',@Z,' ',@M,')'),0);      
    return @POINT.AsBinaryZM();
  end;
  -- Iterative the component geometry and points
  return @GEOMETRY.AsBinaryZM();
end
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_geometryAsBinaryZM'
GO
--
CREATE OR ALTER FUNCTION sp_geographyAsTextZM(@GEOGRAPHY GEOGRAPHY,@SPATIAL_PRECISION int)
returns VARCHAR(MAX)
as
begin
  return case
    when @GEOGRAPHY is NULL then
      NULL
    else
      GEOGRAPHY::STGeomFromText(master.dbo.sp_RoundGeography(@GEOGRAPHY,@SPATIAL_PRECISION),@GEOGRAPHY.STSrid).AsTextZM()
  end
end
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_geographyAsTextZM'
GO
--
CREATE OR ALTER FUNCTION sp_geographyAsBinaryZM(@GEOGRAPHY GEOGRAPHY,@SPATIAL_PRECISION int)
returns VARBINARY(MAX)
as
begin
  return case
    when @GEOGRAPHY is NULL then
      NULL
    else
      GEOGRAPHY::STGeomFromText(master.dbo.sp_RoundGeography(@GEOGRAPHY,@SPATIAL_PRECISION),@GEOGRAPHY.STSrid).AsBinaryZM()
  end;
end
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_geographyAsBinaryZM'
GO
--
CREATE OR ALTER PROCEDURE sp_COMPARE_SCHEMA(@FORMAT_RESULTS BIT,@SOURCE_DATABASE NVARCHAR(128), @SOURCE_SCHEMA NVARCHAR(128), @TARGET_DATABASE NVARCHAR(128), @TARGET_SCHEMA NVARCHAR(128), @COMMENT NVARCHAR(2048), @EMPTY_STRING_IS_NULL BIT, @SPATIAL_PRECISION int, @DATE_TIME_PRECISION int) 
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @COLUMN_LIST      NVARCHAR(MAX);
  DECLARE @SQL_STATEMENT    NVARCHAR(MAX);
  DECLARE @BAD_STATEMENT    NVARCHAR(MAX);
  DECLARE @C_NEWLINE        CHAR(1) = CHAR(10);
  
  DECLARE @SOURCE_COUNT BIGINT;
  DECLARE @TARGET_COUNT BIGINT;
  DECLARE @SQLERRM     NVARCHAR(2000);
  
  DECLARE FETCH_METADATA 
  CURSOR FOR 
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
                          else
                            concat('master.dbo.sp_geographyAsBinaryZM("',c.COLUMN_NAME,'",',@SPATIAL_PRECISION,') "',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('geometry') then
                        -- concat('CONVERT(varchar(max), "',c.COLUMN_NAME,'".AsBinaryZM(),2) "',c.COLUMN_NAME,'"')
                        case 
                          when @SPATIAL_PRECISION = 18 then
                            concat('"',c.COLUMN_NAME,'".AsBinaryZM() "',c.COLUMN_NAME,'"')
                          else
                            concat('master.dbo.sp_geometryAsBinaryZM("',c.COLUMN_NAME,'",',@SPATIAL_PRECISION,') "',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('xml','text','ntext') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.COLUMN_NAME,'" as NVARCHAR(MAX))) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('image') then
                        concat('HASHBYTES(''SHA2_256'',CAST("',c.COLUMN_NAME,'" as VARBINARY(MAX))) "',c.COLUMN_NAME,'"')
				      else  
                        concat('"',c.COLUMN_NAME,'"')
                      end
                   ,',') 
         within group (order by ordinal_position) "columns"
   from INFORMATION_SCHEMA.COLUMNS c, INFORMATION_SCHEMA.TABLES t
  where t.TABLE_NAME = c.TABLE_NAME
    and t.TABLE_SCHEMA = c.TABLE_SCHEMA
    and t.TABLE_TYPE = 'BASE TABLE'
    and t.TABLE_SCHEMA = @SOURCE_SCHEMA
    -- and t.table_catalog = @SOURCE_DATABASE
  group by t.TABLE_SCHEMA, t.TABLE_NAME;
 
  SET QUOTED_IDENTIFIER ON; 
  
  
  CREATE TABLE #SCHEMA_COMPARE_RESULTS (
    SOURCE_DATABASE  NVARCHAR(128)
   ,SOURCE_SCHEMA    NVARCHAR(128)
   ,TARGET_DATABASE  NVARCHAR(128)
   ,TARGET_SCHEMA    NVARCHAR(128)
   ,TABLE_NAME       NVARCHAR(128)
   ,SOURCE_ROW_COUNT BIGINT
   ,TARGET_ROW_COUNT BIGINT
   ,MISSING_ROWS     BIGINT
   ,EXTRA_ROWS       BIGINT
   ,SQLERRM          NVARCHAR(2048)
   ,SQL_STATEMENT    NVARCHAR(MAX)
  );

  SET NOCOUNT ON;

  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @COLUMN_LIST
  WHILE @@FETCH_STATUS = 0 
  BEGIN    
  
    SET @SQL_STATEMENT = CONCAT('insert into #SCHEMA_COMPARE_RESULTS ',@C_NEWLINE,
                                ' select ''',@SOURCE_DATABASE,''' ',@C_NEWLINE,
                                '       ,''',@SOURCE_SCHEMA,''' ',@C_NEWLINE,
                                '       ,''',@TARGET_DATABASE,''' ',@C_NEWLINE,
                                '       ,''',@TARGET_SCHEMA,''' ',@C_NEWLINE,
                                '       ,''',@TABLE_NAME,''' ',@C_NEWLINE,
                                '       ,(select count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'")',@C_NEWLINE,
                                '       ,(select count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'")',@C_NEWLINE,
                                '       ,(select count(*) from (SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'") T1)',@C_NEWLINE,
                                '       ,(select count(*) from (SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" EXCEPT SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'") T2)',@C_NEWLINE,
                                '       ,NULL,NULL');
							 
    BEGIN TRY 
      -- SELECT @SQL_STATEMENT;
      EXEC(@SQL_STATEMENT)
    END TRY
    BEGIN CATCH
      if (ERROR_NUMBER() = 41317) 
      BEGIN

        -- A user transaction that accesses memory optimized tables or natively compiled modules cannot access more than one user database or databases model and msdb, and it cannot write to master

        DECLARE @SOURCE_HASH_BUCKET TABLE(HASH VARBINARY(8000));
        DECLARE @TARGET_HASH_BUCKET TABLE(HASH VARBINARY(8000));
        DECLARE @MISSING_ROWS BIGINT;
        DECLARE @EXTRA_ROWS   BIGINT;

        BEGIN TRY
                    
          SET @SQL_STATEMENT = CONCAT('WITH XML_TABLE(XML_DOC) AS (SELECT ',@COLUMN_LIST,' FROM "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'" FOR XML RAW, ELEMENTS XSINIL, BINARY BASE64, TYPE )',@C_NEWLINE,
                                      'SELECT HASHBYTES(''SHA2_256'',CAST(T2.ROW_XML.query(''.'') AS NVARCHAR(MAX))) from XML_TABLE CROSS APPLY XML_DOC.nodes(''/Row'') as T2(ROW_XML)');
                                                                      
          INSERT INTO @SOURCE_HASH_BUCKET 
          EXEC(@SQL_STATEMENT)
       
          SET @SQL_STATEMENT = CONCAT('WITH XML_TABLE(XML_DOC) AS (SELECT ',@COLUMN_LIST,' FROM "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'" FOR XML RAW, ELEMENTS XSINIL, BINARY BASE64, TYPE )',@C_NEWLINE,
                                      'SELECT HASHBYTES(''SHA2_256'',CAST(T2.ROW_XML.query(''.'') AS NVARCHAR(MAX))) from XML_TABLE CROSS APPLY XML_DOC.nodes(''/Row'') as T2(ROW_XML)');

          INSERT INTO @TARGET_HASH_BUCKET 
          EXEC(@SQL_STATEMENT)
          
          select @MISSING_ROWS = count(*) from (SELECT HASH FROM @SOURCE_HASH_BUCKET EXCEPT SELECT HASH FROM @TARGET_HASH_BUCKET) T1;
          select @EXTRA_ROWS = count(*) from (SELECT HASH FROM @TARGET_HASH_BUCKET EXCEPT SELECT HASH FROM @SOURCE_HASH_BUCKET) T1;

          SET @SQL_STATEMENT = CONCAT('select @SOURCE_COUNT = count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"')
          EXEC sp_EXECUTESQL @SQL_STATEMENT ,N'@SOURCE_COUNT BIGINT OUTPUT', @SOURCE_COUNT OUTPUT
          
          SET @SQL_STATEMENT = CONCAT('select @TARGET_COUNT = count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"')
          EXEC sp_EXECUTESQL @SQL_STATEMENT,N'@TARGET_COUNT BIGINT OUTPUT', @TARGET_COUNT OUTPUT
          
          INSERT INTO #SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, @MISSING_ROWS, @EXTRA_ROWS, NULL, NULL)
        END TRY
        BEGIN CATCH
          SET @BAD_STATEMENT = @SQL_STATEMENT
          SET @SQLERRM = CONCAT(ERROR_NUMBER(),': ',ERROR_MESSAGE())
        END CATCH
      END
      ELSE 
      BEGIN
        SET @BAD_STATEMENT = @SQL_STATEMENT
        SET @SQLERRM = CONCAT(ERROR_NUMBER(),': ',ERROR_MESSAGE())
      END
    END CATCH
    
    IF (@SQLERRM IS NOT NULL)
    BEGIN
      BEGIN TRY
        SET @SQL_STATEMENT = CONCAT('select @SOURCE_COUNT = count(*) from "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'"."',@TABLE_NAME,'"')
        EXEC sp_EXECUTESQL @SQL_STATEMENT ,N'@SOURCE_COUNT BIGINT OUTPUT', @SOURCE_COUNT OUTPUT
      END TRY
      BEGIN CATCH
         SET @SQLERRM =  CONCAT(ERROR_NUMBER(),': ',ERROR_MESSAGE(),'. ',@SQL_STATEMENT)
         SET @SOURCE_COUNT = -1
      END CATCH
      BEGIN TRY
        SET @SQL_STATEMENT = CONCAT('select @TARGET_COUNT = count(*) from "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'"."',@TABLE_NAME,'"')
        EXEC sp_EXECUTESQL @SQL_STATEMENT,N'@TARGET_COUNT BIGINT OUTPUT', @TARGET_COUNT OUTPUT
      END TRY
      BEGIN CATCH
        SET @SQLERRM =  CONCAT(ERROR_NUMBER(),': ',ERROR_MESSAGE(),'. ',@SQL_STATEMENT)
        SET @TARGET_COUNT = -1
      END CATCH
      INSERT INTO #SCHEMA_COMPARE_RESULTS VALUES (@SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, @TABLE_NAME, @SOURCE_COUNT, @TARGET_COUNT, -1, -1,@SQLERRM,@BAD_STATEMENT)
      SET @SQLERRM = NULL
    END
    FETCH FETCH_METADATA INTO @TABLE_NAME, @COLUMN_LIST
  END
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
  
  SELECT @SOURCE_COUNT = COUNT(*) 
    FROM #SCHEMA_COMPARE_RESULTS
   where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
      or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
      or SQLERRM is not NULL
 
  IF (@FORMAT_RESULTS = 1) 
  BEGIN

    SELECT CAST(CONCAT( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "',@SOURCE_DATABASE,'","',@TARGET_DATABASE,'", ',@COMMENT) as NVARCHAR(256)) " "
  
    SET NOCOUNT OFF;

    SELECT CAST(FORMATMESSAGE('%32s %32s %48s %16s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, cast(TARGET_ROW_COUNT as VARCHAR(16))) as NVARCHAR(256)) "SUCCESS          Source Schenma                    Target Schema                                            Table          Rows"
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
--
    IF (@SOURCE_COUNT > 0) 
    BEGIN
      SELECT CAST(FORMATMESSAGE('%32s %32s %48s %16s %16s %16s %16s %64s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, cast(SOURCE_ROW_COUNT as VARCHAR(16)), cast(TARGET_ROW_COUNT as VARCHAR(16)), cast(MISSING_ROWS as VARCHAR(16)), cast(EXTRA_ROWS as VARCHAR(16)), SQLERRM) as NVARCHAR(256)) "FAILED           Source Schenma                    Target Schema                                            Table Details..."
        FROM #SCHEMA_COMPARE_RESULTS
       where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
          or MISSING_ROWS <> 0
         or EXTRA_ROWS <> 0
         or SQLERRM is not NULL
      order by TABLE_NAME;
    END;
  END
  ELSE 
  BEGIN
    SELECT SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
  
    SELECT SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
        or MISSING_ROWS <> 0
       or EXTRA_ROWS <> 0
        or SQLERRM is not NULL
     order by TABLE_NAME;
  END
--
END
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_COMPARE_SCHEMA'
GO
--
EXIT