use master
go
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
                        concat('cast("',c.COLUMN_NAME,'" as datetime2(',@DATE_TIME_PRECISION,')) "',c.COLUMN_NAME,'"')
                      when c.DATA_TYPE in ('varchar','nvarchar') then
                        case 
                          when @EMPTY_STRING_IS_NULL = 1 then
                            concat('case when "',c.COLUMN_NAME,'" = '''' then NULL else "',c.COLUMN_NAME,'" end collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                          else
                            concat('"',c.COLUMN_NAME,'" collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                        end
                      when c.DATA_TYPE in ('char','nchar') then
                        concat('"',c.COLUMN_NAME,'" collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')					  
                      when c.DATA_TYPE in ('geography','geometry') then
                        case 
                           when @SPATIAL_PRECISION is NULL then
                             concat('"',c.COLUMN_NAME,'".ToString() collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                           else
                             case 
                               when c.DATA_TYPE = 'geography' then
                                 concat('case when "',c.COLUMN_NAME,'".MakeValid().InstanceOf(''POINT'') = 1 then concat(str("',c.COLUMN_NAME,'".Long,18,',@SPATIAL_PRECISION,'),'','',str("',c.COLUMN_NAME,'".Lat,18,',@SPATIAL_PRECISION,')) else "',c.COLUMN_NAME,'".ToString() end collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                               else
                                 concat('case when "',c.COLUMN_NAME,'".MakeValid().InstanceOf(''POINT'') = 1 then concat(str("',c.COLUMN_NAME,'".STX,18,',@SPATIAL_PRECISION,'),'','',str("',c.COLUMN_NAME,'".STY,18,',@SPATIAL_PRECISION,')) else "',c.COLUMN_NAME,'".ToString() end collate DATABASE_DEFAULT "',c.COLUMN_NAME,'"')
                               end
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

    SELECT CAST(FORMATMESSAGE('%32s %32s %48s %12i', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT) as NVARCHAR(256)) "SUCCESS          Source Schenma                    Target Schema                                            Table          Rows"
      FROM #SCHEMA_COMPARE_RESULTS
     where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
       and MISSING_ROWS = 0
       and EXTRA_ROWS = 0
       and SQLERRM is NULL
    order by TABLE_NAME;
--
    IF (@SOURCE_COUNT > 0) 
    BEGIN
      SELECT CAST(FORMATMESSAGE('%32s %32s %48s   %12i %12i %12i %12i %64s', SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM) as NVARCHAR(256)) "FAILED           Source Schenma                    Target Schema                                            Table Details..."
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
  -- SELECT SQL_STATEMENT from #SCHEMA_COMPARE_RESULTS
  -- SELECT @BAD_STATEMENT
--
END
--
GO
--
EXECUTE sp_ms_marksystemobject 'sp_COMPARE_SCHEMA'
GO
--
EXIT