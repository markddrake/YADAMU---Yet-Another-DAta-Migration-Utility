CREATE OR ALTER FUNCTION MAP_FOREIGN_DATATYPE(@DATA_TYPE VARCHAR(128), @DATA_TYPE_LENGTH INT, @DATA_TYPE_SCALE INT) 
RETURNS VARCHAR(128) 
AS
BEGIN
  RETURN CASE
	when @DATA_TYPE = 'VARCHAR2' 
	  then 'varchar'
	when @DATA_TYPE = 'NVARCHAR2' 
	  then 'nvarchar'
	when @DATA_TYPE = 'CLOB'
      then 'varchar'
	when @DATA_TYPE = 'NCLOB'
      then 'nvarchar'
	when @DATA_TYPE = 'NUMBER'
      then 'decimal'
	when @DATA_TYPE = 'BINARY_DOUBLE'
      then 'float'
	when @DATA_TYPE = 'BINARY_FLOAT'
      then 'real'
	when @DATA_TYPE = 'RAW'
      then 'binary'
	when @DATA_TYPE = 'BLOB'
      then 'varbinary'
	when (CHARINDEX('TIMESTAMP',@DATA_TYPE) = 1) 
	  then case
	         when (CHARINDEX('TIME ZONE',@DATA_TYPE) > 0) 
			   then 'datetimeoffset'
			   else 'datetime2' 
           end
	when (CHARINDEX('XMLTYPE',@DATA_TYPE) > 0) 
	  then 'xml'
	when (CHARINDEX('"."',@DATA_TYPE) > 0) 
	  then 'nvarchar'
	else
	  @DATA_TYPE
 /*  
   	  if ((instr(V_DATA_TYPE,'INTERVAL') = 1)) then
	    return 'VARCHAR(16)';
      end if;
	  return @DATA_TYPE;
*/
  END;
END;
GO
--
CREATE OR ALTER PROCEDURE GENERATE_STATEMENTS(@SCHEMA NVARCHAR(128), @TABLE_NAME NVARCHAR(128), @COLUMN_LIST NVARCHAR(MAX),@DATA_TYPE_LIST NVARCHAR(MAX),@DATA_SIZE_LIST NVARCHAR(MAX),@DDL_STATEMENT NVARCHAR(MAX) OUTPUT, @DML_STATEMENT NVARCHAR(MAX) OUTPUT) 
AS
BEGIN
  DECLARE @COLUMNS_CLAUSE     NVARCHAR(MAX);
  DECLARE @INSERT_SELECT_LIST NVARCHAR(MAX);
  DECLARE @WITH_CLAUSE        NVARCHAR(MAX);
 
  WITH "SOURCE_TABLE_DEFINITION" as (
          SELECT c."KEY" "INDEX"
	            ,c."VALUE" "COLUMN_NAME"
		        ,t."VALUE" "DATA_TYPE"
		        ,CASE
				   WHEN s.VALUE = ''
				     THEN NULL
	               WHEN CHARINDEX(',',s."VALUE") > 0 
                     THEN LEFT(s."VALUE",CHARINDEX(',',s."VALUE")-1)
                   ELSE
  				     s."VALUE"
		         END "DATA_TYPE_LENGTH"
		        ,CASE
	               WHEN CHARINDEX(',',s."VALUE") > 0 
			         THEN RIGHT(s."VALUE", CHARINDEX(',',REVERSE(s."VALUE"))-1)
			       ELSE 
				     NULL
		         END "DATA_TYPE_SCALE"
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
		                   CASE
				             WHEN "DATA_TYPE" = 'BFILE'
                               THEN 'VARCHAR(2048)'				  
				             WHEN "DATA_TYPE" in ('ROWID','UROWID') 
                               THEN 'VARCHAR(18)'				  
				             WHEN "DATA_TYPE" in('xml','text','ntext','image','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney')
                               THEN "DATA_TYPE"				  
				             WHEN (CHARINDEX('INTERVAL',"DATA_TYPE") = 1)
                               THEN 'VARCHAR(64)'				  
				             WHEN "DATA_TYPE_LENGTH" = -1 OR (DATA_TYPE in ('CLOB','NCLOB','BLOB'))
					           THEN  CONCAT("TARGET_DATA_TYPE",'(max)')
					         WHEN "DATA_TYPE_SCALE" IS NOT NULL
					           THEN CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
				             WHEN "DATA_TYPE_LENGTH"  IS NOT NULL 
					          THEN CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
				             ELSE 
					           "TARGET_DATA_TYPE"
            		       END
					     ) 
                   ,','					
				   )
       ,@INSERT_SELECT_LIST = 
	     STRING_AGG(CASE
                      WHEN "TARGET_DATA_TYPE" in ('binary','varbinary')
		                THEN CASE
            				    WHEN ((DATA_TYPE_LENGTH = -1) OR (DATA_TYPE = 'BLOB') or ((CHARINDEX('"."',DATA_TYPE) > 0)))
							      THEN CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(max),"',"COLUMN_NAME",'") "',COLUMN_NAME,'"')
		                          ELSE CONCAT('CONVERT(',"TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",'),"',"COLUMN_NAME",'") "',COLUMN_NAME,'"')
							 END
                      WHEN "TARGET_DATA_TYPE" = 'image'
					    THEN CONCAT('convert(varchar(max),CONVERT(varbinary(max),"',"COLUMN_NAME",'"),2)')
					  ELSE
					    CONCAT('"',"COLUMN_NAME",'"')
				    END 
                   ,','					
				   )
	   ,@WITH_CLAUSE =
	    STRING_AGG(CONCAT('"',COLUMN_NAME,'" ',
         		          CASE
				            WHEN "DATA_TYPE" = 'BFILE'
                              THEN 'VARCHAR(2048)'				  
				            WHEN  "DATA_TYPE" in ('ROWID','UROWID') 
                              THEN 'VARCHAR(18)'				  
				            WHEN (CHARINDEX('INTERVAL',"DATA_TYPE") = 1)
                              THEN 'VARCHAR(64)'				  
  			                WHEN "DATA_TYPE" in('xml','real','double precision','tinyint','smallint','int','bigint','bit','date','datetime','money','smallmoney')
                              THEN "DATA_TYPE"		  
				            WHEN "DATA_TYPE" = 'image'
                              THEN 'nvarchar(max)'				  
				            WHEN "DATA_TYPE" = 'text'
                              THEN 'varchar(max)'				  
				            WHEN "DATA_TYPE" = 'ntext'
                              THEN 'nvarchar(max)'				  
                            WHEN "TARGET_DATA_TYPE" in ('varchar','nvarchar') 
					          THEN CASE
						             WHEN ((DATA_TYPE_LENGTH = -1) or (DATA_TYPE in ('CLOB','NCLOB')) or ((CHARINDEX('"."',DATA_TYPE) > 0)))
							           THEN CONCAT("TARGET_DATA_TYPE",'(max)')
		                               ELSE CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
						           END
                            WHEN "TARGET_DATA_TYPE" in ('binary','varbinary') 
					          THEN CASE
						             WHEN DATA_TYPE_LENGTH = -1 OR DATA_TYPE = 'BLOB'
							           THEN 'varchar(max)'
		                               ELSE CONCAT('varchar(',cast(("DATA_TYPE_LENGTH" * 2) as VARCHAR),')')
						           END
					        WHEN "DATA_TYPE_SCALE" IS NOT NULL
					          THEN CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",',', "DATA_TYPE_SCALE",')')
				            WHEN "DATA_TYPE_LENGTH" IS NOT NULL 
					          THEN CONCAT("TARGET_DATA_TYPE",'(',"DATA_TYPE_LENGTH",')')
				            ELSE 
					          "TARGET_DATA_TYPE"
				          END,
					      ' ''$[',st."INDEX",']'''
					    )
                        ,','					
				   )
      FROM "SOURCE_TABLE_DEFINITION" st, "TARGET_TABLE_DEFINITION" tt 
     where st."INDEX" = tt."INDEX";	  
	 
   SET @DDL_STATEMENT = CONCAT('if object_id(''"',@SCHEMA,'"."',@TABLE_NAME,'"'',''U'') is NULL create table "',@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMNS_CLAUSE,')');
   SET @DML_STATEMENT = CONCAT('insert into "' ,@SCHEMA,'"."',@TABLE_NAME,'" (',@COLUMN_LIST,') select ',@INSERT_SELECT_LIST,'  from "JSON_STAGING" CROSS APPLY OPENJSON("DATA",''$.data."',@TABLE_NAME,'"'') WITH ( ',@WITH_CLAUSE,') data');
END;
GO
--
CREATE OR ALTER PROCEDURE IMPORT_JSON(@TARGET_DATABASE VARCHAR(128)) 
AS
BEGIN
  DECLARE @RESULTS        TABLE(
                              "TABLE_NAME"    VARCHAR(128)
						 	 ,"ROW_COUNT"     BIGINT
					 		 ,"ELAPSED_TIME"  BIGINT
							 ,"DDL_STATEMENT" NVARCHAR(MAX)
							 ,"DML_STATEMENT" NVARCHAR(MAX)
							 ,"ERROR"         NVARCHAR(4000)
						    );
  DECLARE @RESULT           NVARCHAR(MAX) = '';
  DECLARE @ERROR            NVARCHAR(4000);
							   
  DECLARE @OWNER            VARCHAR(128);
  DECLARE @TABLE_NAME       VARCHAR(128);
  DECLARE @COLUMN_LIST      NVARCHAR(MAX);
  DECLARE @DATA_TYPE_LIST   NVARCHAR(MAX);
  DECLARE @SIZE_CONSTRAINTS NVARCHAR(MAX);
  DECLARE @DDL_STATEMENT    NVARCHAR(MAX);
  DECLARE @DML_STATEMENT    NVARCHAR(MAX);
  
  DECLARE @START_TIME      DATETIME2;
  DECLARE @END_TIME        DATETIME2;
  DECLARE @ELAPSED_TIME    BIGINT;  
  DECLARE @ROW_COUNT       BIGINT;
  
  DECLARE FETCH_METADATA 
  CURSOR FOR 
  select OWNER
        ,TABLE_NAME
		,COLUMN_LIST
		,DATA_TYPE_LIST
		,SIZE_CONSTRAINTS
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
                      ,COLUMN_PATTERNS              VARCHAR(MAX)  '$.columnPatterns');
 
  EXEC sys.sp_set_session_context 'JSON_IMPORT', 'IN-PROGRESS'
  SET QUOTED_IDENTIFIER ON; 


  CREATE TABLE #RESULTS (
    "TABLE_NAME"    VARCHAR(128)
   ,"ROW_COUNT"     BIGINT
   ,"ELAPSED_TIME"  BIGINT
   ,"DDL_STATEMENT" NVARCHAR(MAX)
   ,"DML_STATEMENT" NVARCHAR(MAX)
   ,"ERROR"         NVARCHAR(4000)
  );
  
  OPEN FETCH_METADATA;
  FETCH FETCH_METADATA INTO @OWNER, @TABLE_NAME, @COLUMN_LIST, @DATA_TYPE_LIST, @SIZE_CONSTRAINTS
  WHILE @@FETCH_STATUS = 0 BEGIN 
     BEGIN TRY 
	   SET @ERROR = '{}';
       SET @DDL_STATEMENT = null;
       SET @DML_STATEMENT = null;
	   EXEC GENERATE_STATEMENTS @TARGET_DATABASE, @TABLE_NAME, @COLUMN_LIST, @DATA_TYPE_LIST, @SIZE_CONSTRAINTS, @DDL_STATEMENT OUTPUT, @DML_STATEMENT OUTPUT
	   SET @ROW_COUNT = 0;
       EXEC(@DDL_STATEMENT)
	   SET @START_TIME = SYSUTCDATETIME();
 	   EXEC(@DML_STATEMENT)
	   SET @ROW_COUNT = @@ROWCOUNT;
	   SET @END_TIME = SYSUTCDATETIME();
     END TRY  
     BEGIN CATCH  
	   SET @END_TIME = SYSUTCDATETIME();
       set @ERROR = (
        select ERROR_PROCEDURE() as [procedure], ERROR_LINE() as [lineNumber], ERROR_MESSAGE() AS [msg], ERROR_NUMBER() as [code], ERROR_SEVERITY() as [severity], ERROR_STATE() as 'state'
  	       for JSON PATH
        )
     END CATCH 
     SET @ELAPSED_TIME = DATEDIFF(MILLISECOND,@START_TIME,@END_TIME);
     INSERT INTO #RESULTS values (@TABLE_NAME, @ROW_COUNT, @ELAPSED_TIME, @DDL_STATEMENT, @DML_STATEMENT, @ERROR);
     FETCH FETCH_METADATA INTO @OWNER, @TABLE_NAME, @COLUMN_LIST, @DATA_TYPE_LIST, @SIZE_CONSTRAINTS
  END;
 
  CLOSE FETCH_METADATA;
  DEALLOCATE FETCH_METADATA;
  
  /* WORKAROUND for client TIMEOUT at 15000ms */
  set @RESULT = (
    select "TABLE_NAME" as [tableName], "ROW_COUNT" as [rowCount], "ELAPSED_TIME" as [elapsedTime], "DDL_STATEMENT" as [ddlStatement], "DML_STATEMENT" as [dmlStatement], "ERROR" as [error] 
    from #RESULTS
     for JSON PATH, INCLUDE_NULL_VALUES
  )
  -- delete from JSON_STAGING;
  -- insert into JSON_STAGING values (@RESULT);

  EXEC sys.sp_set_session_context 'JSON_IMPORT', 'COMPLETE'
  -- WAITFOR DELAY '00:30';  

  /* END WORKAROUND */
  
  select "TABLE_NAME" as [tableName], "ROW_COUNT" as [rowCount], "ELAPSED_TIME" as [elapsedTime], "DDL_STATEMENT" as [ddlStatement], "DML_STATEMENT" as [dmlStatement], "ERROR" as [error] 
    from #rESULTS
	 for JSON PATH, INCLUDE_NULL_VALUES
end;
--
go
--
exit