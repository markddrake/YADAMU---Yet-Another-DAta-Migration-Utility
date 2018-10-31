CREATE OR ALTER FUNCTION MAP_FOREIGN_DATATYPE(@DATA_TYPE VARCHAR(128), @DATA_TYPE_LENGTH INT, @DATA_TYPE_SCALE INT) 
RETURNS VARCHAR(128) 
AS
BEGIN
  RETURN case
	when @DATA_TYPE = 'VARCHAR2' 
	  then 'varchar'
	when @DATA_TYPE = 'NVARCHAR2' 
	  then 'nvarchar'
	when @DATA_TYPE = 'CLOB'
      then 'varchar(max)'
	when @DATA_TYPE = 'NCLOB'
      then 'nvarchar(max)'
	when @DATA_TYPE = 'NUMBER'
      then 'decimal'
	when @DATA_TYPE = 'BINARY_DOUBLE'
      then 'float'
	when @DATA_TYPE = 'BINARY_FLOAT'
      then 'real'
	when @DATA_TYPE = 'RAW'
      then 'varbinary'
	when @DATA_TYPE = 'BLOB'
      then 'varbinary(max)'
    when @DATA_TYPE = 'BFILE'
      then 'varchar(2048)'	
    when @DATA_TYPE in ('ROWID','UROWID') 
      then 'varchar(18)'				  
    when @DATA_TYPE in ('ANYDATA') 
      then 'nvarchar(max)'				  
    when (CHARINDEX('INTERVAL',@DATA_TYPE) = 1)
      then 'varchar(64)'				  
    when (CHARINDEX('TIMESTAMP',@DATA_TYPE) = 1) 
	  then case
	         when (CHARINDEX('TIME ZONE',@DATA_TYPE) > 0) 
			   then 'datetimeoffset'
			   else 'datetime2' 
           end
	when (CHARINDEX('XMLTYPE',@DATA_TYPE) > 0) 
	  then 'xml'
	when (CHARINDEX('"."',@DATA_TYPE) > 0) 
	  then 'nvarchar(max)'
    when @DATA_TYPE = 'mediumint' 
      then 'int'
    when @DATA_TYPE = 'enum'
      then 'varchar(255)'
    when @DATA_TYPE = 'set'
      then 'varchar(255)'
    when @DATA_TYPE = 'year'
      then 'smallint'
    else
	  lower(@DATA_TYPE)
  end
end
--
GO
--
CREATE OR ALTER FUNCTION GENERATE_STATEMENTS(@SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_LIST NVARCHAR(MAX),@DATA_TYPE_LIST NVARCHAR(MAX),@DATA_SIZE_LIST NVARCHAR(MAX)) 
RETURNS NVARCHAR(MAX)
AS
BEGIN
  DECLARE @COLUMNS_CLAUSE     NVARCHAR(MAX);
  DECLARE @INSERT_SELECT_LIST NVARCHAR(MAX);
  DECLARE @WITH_CLAUSE        NVARCHAR(MAX);
  DECLARE @BULK_INSERT_TYPES  NVARCHAR(MAX);
  
  DECLARE @DDL_STATEMENT      NVARCHAR(MAX);
  DECLARE @DML_STATEMENT      NVARCHAR(MAX);
 
  WITH "SOURCE_TABLE_DEFINITION" as (
          SELECT c."KEY" "INDEX"
	            ,c."VALUE" "COLUMN_NAME"
		        ,t."VALUE" "DATA_TYPE"
		        ,case
				   when s.VALUE = ''
				     then NULL
	               when CHARINDEX(',',s."VALUE") > 0 
                     then LEFT(s."VALUE",CHARINDEX(',',s."VALUE")-1)
                   else
  				     s."VALUE"
		         end "DATA_TYPE_LENGTH"
		        ,case
	               when CHARINDEX(',',s."VALUE") > 0 
			         then RIGHT(s."VALUE", CHARINDEX(',',REVERSE(s."VALUE"))-1)
			       else 
				     NULL
		         end "DATA_TYPE_SCALE"
            FROM OPENJSON(CONCAT('[',REPLACE(@COLUMN_LIST,'"."','\".\"'),']')) c,
	             OPENJSON(CONCAT('[',REPLACE(@DATA_TYPE_LIST,'"."','\".\"'),']')) t,
		         OPENJSON(CONCAT('[',REPLACE(@DATA_SIZE_LIST,'"."','\".\"'),']')) s
           WHERE c."KEY" = t."KEY" and c."KEY" = s."KEY"
  ),
  "TARGET_TABLE_DEFINITION" as (
    select "INDEX", dbo.MAP_FOREIGN_DATATYPE("DATA_TYPE","DATA_TYPE_LENGTH","DATA_TYPE_SCALE") TARGET_DATA_TYPE
      from "SOURCE_TABLE_DEFINITION"
  )
  SELECT @COLUMNS_CLAUSE =
         STRING_AGG(CONCAT('"',"COLUMN_NAME",'" ',
		                   case
                             when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0)  
                               then "TARGET_DATA_TYPE"
				             when "TARGET_DATA_TYPE" in('xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney','geography','geometry','hierarchyid','uniqueidentifier')
                               then "TARGET_DATA_TYPE"				  
				             when "DATA_TYPE_SCALE" IS NOT NULL
					           then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
				             when "DATA_TYPE_LENGTH"  IS NOT NULL 
                               then case 
                                      when "DATA_TYPE_LENGTH" = -1 
                                        then CONCAT("TARGET_DATA_TYPE",'(max)')
					                    else CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                                    end
				             else 
					           "TARGET_DATA_TYPE"
            		       end
					     ) 
                   ,','					
				   )
        ,@BULK_INSERT_TYPES =
         STRING_AGG(CONCAT('"',
                           case
                             when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0)  
                               then "TARGET_DATA_TYPE"
			                 when "TARGET_DATA_TYPE" in('xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney')
                               then "TARGET_DATA_TYPE"				  
	                         when "DATA_TYPE_SCALE" IS NOT NULL
					           then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
				             when "DATA_TYPE_LENGTH"  IS NOT NULL 
                               then case 
                                      when "DATA_TYPE_LENGTH" = -1 
                                        then CONCAT("TARGET_DATA_TYPE",'(max)')
					                    else CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                                    end
				             else 
				               "TARGET_DATA_TYPE"
            	           end,
                           '"'
                          )
                   ,','					
				   )
        ,@INSERT_SELECT_LIST = 
	     STRING_AGG(case
                      when "TARGET_DATA_TYPE" in ('binary','varbinary')
		                then case
            				    when ((DATA_TYPE_LENGTH = -1) OR (DATA_TYPE = 'BLOB') or ((CHARINDEX('"."',DATA_TYPE) > 0)))
							      then CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(max),"',"COLUMN_NAME",'") "',COLUMN_NAME,'"')
		                          else CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",'),"',"COLUMN_NAME",'") "',COLUMN_NAME,'"')
							 end
                      when "TARGET_DATA_TYPE" = 'image'
					    then CONCAT('convert(varchar(max),CONVERT(varbinary(max),"',"COLUMN_NAME",'"),2)')
					  else
					    CONCAT('"',"COLUMN_NAME",'"')
				    end 
                   ,','					
				   )
	   ,@WITH_CLAUSE =
	    STRING_AGG(CONCAT('"',COLUMN_NAME,'" ',
         		          case			  
                            when (CHARINDEX('(',"TARGET_DATA_TYPE") > 0)  
                              then "TARGET_DATA_TYPE"
  			                when "TARGET_DATA_TYPE" in('xml','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney')
                              then "TARGET_DATA_TYPE"		  
				            when "TARGET_DATA_TYPE" = 'image'
                              then 'nvarchar(max)'				  
				            when "TARGET_DATA_TYPE" = 'text'
                              then 'varchar(max)'				  
				            when "TARGET_DATA_TYPE" = 'ntext'
                              then 'nvarchar(max)'				  
                            when "TARGET_DATA_TYPE" in ('varchar','nvarchar') 
					          then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
                            when "TARGET_DATA_TYPE" in ('binary','varbinary') 
					          then CONCAT('varchar(',cast(("DATA_TYPE_LENGTH" * 2) as VARCHAR),')')
					        when "DATA_TYPE_SCALE" IS NOT NULL
					          then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
				            when "DATA_TYPE_LENGTH" IS NOT NULL 
					          then CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
				            else 
					          "TARGET_DATA_TYPE"
				          end,
					      ' ''$[',st."INDEX",']'''
					    )
                        ,','					
				   )
      FROM "SOURCE_TABLE_DEFINITION" st, "TARGET_TABLE_DEFINITION" tt 
     where st."INDEX" = tt."INDEX";	  
	 
   SET @DDL_STATEMENT = CONCAT('if object_id(''"',@SCHEMA,'"."',@TABLE_NAME,'"'',''U'') is NULL create table "',@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMNS_CLAUSE,')');
   SET @DML_STATEMENT = CONCAT('insert into "' ,@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMN_LIST,') select ',@INSERT_SELECT_LIST,'  from "JSON_STAGING" CROSS APPLY OPENJSON("DATA",''$.data."',@TABLE_NAME,'"'') WITH ( ',@WITH_CLAUSE,') data');
   RETURN JSON_MODIFY(JSON_MODIFY(JSON_MODIFY('{}','$.ddl',@DDL_STATEMENT),'$.dml',@DML_STATEMENT),'$.targetDataTypes',@BULK_INSERT_TYPES)
end;
GO
--
CREATE OR ALTER PROCEDURE IMPORT_JSON(@TARGET_DATABASE VARCHAR(128)) 
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @STATEMENTS       NVARCHAR(MAX);
  DECLARE @SQL_STATEMENT    NVARCHAR(MAX);
  
  DECLARE @START_TIME       DATETIME2;
  DECLARE @end_TIME         DATETIME2;
  DECLARE @ELAPSED_TIME     BIGINT;  
  DECLARE @ROW_COUNT        BIGINT;
 
  DECLARE @LOG_ENTRY        NVARCHAR(MAX);
  DECLARE @RESULTS          TABLE(
                              "LOG_ENTRY"      NVARCHAR(MAX)
						    );
                            
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select TABLE_NAME, 
         dbo.GENERATE_STATEMENTS(@TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_LIST, v.DATA_TYPE_LIST, v.SIZE_CONSTRAINTS) as STATEMENTS
   from "JSON_STAGING"
	     CROSS APPLY OPENJSON("DATA", '$.metadata') x
		 CROSS APPLY OPENJSON(x.VALUE) 
		             WITH(
					   OWNER                        VARCHAR(128)  '$.owner'
			          ,TABLE_NAME                   VARCHAR(128)  '$.tableName'
			          ,COLUMN_LIST                  VARCHAR(MAX)  '$.columns'
			          ,DATA_TYPE_LIST               VARCHAR(MAX)  '$.dataTypes'
			          ,SIZE_CONSTRAINTS             VARCHAR(MAX)  '$.dataTypeSizing'
			          ,INSERT_SELECT_LIST           VARCHAR(MAX)  '$.insertSelectList'
                      ,COLUMN_PATTERNS              VARCHAR(MAX)  '$.columnPatterns') v;
 
  SET QUOTED_IDENTIFIER ON; 
  BEGIN TRY
    EXEC sys.sp_set_session_context 'JSON_IMPORT', 'IN-PROGRESS'
    OPEN FETCH_METADATA;
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

    WHILE @@FETCH_STATUS = 0 
    BEGIN 
      SET @ROW_COUNT = 0;
      SET @SQL_STATEMENT = JSON_VALUE(@STATEMENTS,'$.ddl')
      BEGIN TRY 
        EXEC(@SQL_STATEMENT)
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [ddl.tableName], @SQL_STATEMENT as [ddl.sqlStatement] 
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        INSERT INTO @RESULTS VALUES (@LOG_ENTRY)
      end TRY
      BEGIN CATCH  
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [error.tableName], @SQL_STATEMENT as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as 'msg'
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        INSERT INTO @RESULTS VALUES (@LOG_ENTRY)
  	  end CATCH
      
      BEGIN TRY 
        SET @START_TIME = SYSUTCDATETIME();
        SET @SQL_STATEMENT = JSON_VALUE(@STATEMENTS,'$.ddl')
   	    EXEC(@SQL_STATEMENT)
        SET @ROW_COUNT = @@ROWCOUNT;
   	    SET @end_TIME = SYSUTCDATETIME();
        SET @ELAPSED_TIME = DATEDIFF(MILLISECOND,@START_TIME,@end_TIME);
     	SET @LOG_ENTRY = (
          select @TABLE_NAME as [dml.tableName], @ROW_COUNT as [dml.rowCount], @ELAPSED_TIME as [dml.elapsedTime], @SQL_STATEMENT  as [dml.sqlStatement]
             for JSON PATH, INCLUDE_NULL_VALUES
          )
        INSERT INTO @RESULTS VALUES (@LOG_ENTRY)
      end TRY  
      BEGIN CATCH  
        SET @LOG_ENTRY = (
          select @TABLE_NAME as [error.tableName],@SQL_STATEMENT  as [error.sqlStatement], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as 'msg'
             for JSON PATH, INCLUDE_NULL_VALUES
        )
        INSERT INTO @RESULTS VALUES(@LOG_ENTRY);
      end CATCH

      FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
    end;
   
    CLOSE FETCH_METADATA;
    DEALLOCATE FETCH_METADATA;
    
    EXEC sys.sp_set_session_context 'JSON_IMPORT', 'COMPLETE'
   
  end TRY 
  BEGIN CATCH
    SET @LOG_ENTRY = (
      select 'IMPORT_JSON' as [error.tableName], ERROR_NUMBER() as [error.code], ERROR_MESSAGE() as 'msg'
        for JSON PATH, INCLUDE_NULL_VALUES
    )
    INSERT INTO @RESULTS VALUES (@LOG_ENTRY)
  end CATCH
--
  SELECT "LOG_ENTRY" FROM @RESULTS;
end
--
GO
--
CREATE OR ALTER FUNCTION GENERATE_SQL(@TARGET_DATABASE VARCHAR(128),@METADATA NVARCHAR(MAX)) 
returns NVARCHAR(MAX)
AS
BEGIN
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @STATEMENTS       NVARCHAR(MAX);
  
  DECLARE @RESULTS          NVARCHAR(MAX) = '{}'
                            
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select TABLE_NAME, 
         dbo.GENERATE_STATEMENTS(@TARGET_DATABASE, v.TABLE_NAME, v.COLUMN_LIST, v.DATA_TYPE_LIST, v.SIZE_CONSTRAINTS) as STATEMENTS
   from  OPENJSON(@METADATA, '$.metadata') x
		 CROSS APPLY OPENJSON(x.VALUE) 
		             WITH(
					   OWNER                        VARCHAR(128)  '$.owner'
			          ,TABLE_NAME                   VARCHAR(128)  '$.tableName'
			          ,COLUMN_LIST                  VARCHAR(MAX)  '$.columns'
			          ,DATA_TYPE_LIST               VARCHAR(MAX)  '$.dataTypes'
			          ,SIZE_CONSTRAINTS             VARCHAR(MAX)  '$.dataTypeSizing'
			          ,INSERT_SELECT_LIST           VARCHAR(MAX)  '$.insertSelectList'
                      ,COLUMN_PATTERNS              VARCHAR(MAX)  '$.columnPatterns') v;
 
  SET QUOTED_IDENTIFIER ON; 
  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS

  WHILE @@FETCH_STATUS = 0 
  BEGIN 
    SET @RESULTS = JSON_MODIFY(@RESULTS,concat('lax $."',@TABLE_NAME,'"'),@STATEMENTS)
    FETCH FETCH_METADATA INTO @TABLE_NAME, @STATEMENTS
  end;
   
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
    
  RETURN @RESULTS;
end
--
GO
--
EXIT