CONNECT to YADAMU
/
BEGIN 
  DECLARE YADAMU_INSTANCE_ID     VARCHAR(36);
  DECLARE FUNCTION_DEFINITION    VARCHAR(256);
  DECLARE DROP_SCHEMA            VARCHAR(256) DEFAULT 'drop schema "YADAMU" RESTRICT'; 
  DECLARE CREATE_SCHEMA          VARCHAR(256) DEFAULT 'create schema "YADAMU"'; 
  DECLARE INSTALLATION_TIMESTAMP VARCHAR(32);
  
  DECLARE RECREATE_SCHEMA        BOOLEAN DEFAULT TRUE;
  
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN  END;  

  DECLARE CONTINUE HANDLER FOR SQLSTATE '42884', SQLSTATE '56098' 
  
  BEGIN
    SELECT  LEFT(TRANSLATE ( CHAR(BIGINT(RAND() * 10000000000 )), 'abcdef123456789', '1234567890' ),8)
    CONCAT '-'
    CONCAT LEFT(TRANSLATE ( CHAR(BIGINT(RAND() * 10000000000 )), 'abcdef123456789', '1234567890' ),4)
    CONCAT '-'
    CONCAT LEFT(TRANSLATE ( CHAR(BIGINT(RAND() * 10000000000 )), 'abcdef123456789', '1234567890' ),4)
    CONCAT '-'
    CONCAT LEFT(TRANSLATE ( CHAR(BIGINT(RAND() * 10000000000 )), 'abcdef123456789', '1234567890' ),4)
    CONCAT '-'
    CONCAT LEFT(TRANSLATE ( CHAR(BIGINT(RAND() * 10000000000000 )), 'abcdef123456789', '1234567890' ),12)
    INTO YADAMU_INSTANCE_ID
    FROM SYSIBM.SYSDUMMY1;
  END;

  DECLARE CONTINUE HANDLER FOR SQLSTATE '42893' BEGIN SET RECREATE_SCHEMA = FALSE; END;
  

  PREPARE S1 FROM 'SET ? = YADAMU.YADAMU_INSTANCE_ID()';
  EXECUTE S1 INTO YADAMU_INSTANCE_ID;

  FOR D AS 
    SELECT 'DROP FUNCTION "YADAMU"."' || FUNCNAME || '"' AS DROP_STATEMENT 
      FROM SYSCAT.FUNCTIONS
     WHERE FUNCSCHEMA = 'YADAMU' 
  DO 
    EXECUTE IMMEDIATE D.DROP_STATEMENT; 
  END FOR; 
  
  IF (RECREATE_SCHEMA) THEN
    EXECUTE IMMEDIATE DROP_SCHEMA; 
    EXECUTE IMMEDIATE CREATE_SCHEMA; 
  END IF;
  
  SET FUNCTION_DEFINITION = 'CREATE OR REPLACE FUNCTION YADAMU.YADAMU_INSTANCE_ID() RETURNS VARCHAR(36) DETERMINISTIC NO EXTERNAL ACTION CONTAINS SQL RETURN ''' CONCAT UPPER(YADAMU_INSTANCE_ID) CONCAT '''';
  EXECUTE IMMEDIATE FUNCTION_DEFINITION;

  SET INSTALLATION_TIMESTAMP = TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD-HH24:MI:SS.FF6');
  SET INSTALLATION_TIMESTAMP = SUBSTR(INSTALLATION_TIMESTAMP,1,10) CONCAT 'T' CONCAT SUBSTR(INSTALLATION_TIMESTAMP,12);
   
  SET FUNCTION_DEFINITION = 'CREATE OR REPLACE FUNCTION YADAMU.YADAMU_INSTALLATION_TIMESTAMP() RETURNS VARCHAR(36) DETERMINISTIC NO EXTERNAL ACTION CONTAINS SQL RETURN ''' CONCAT INSTALLATION_TIMESTAMP CONCAT '''';
  EXECUTE IMMEDIATE FUNCTION_DEFINITION;
END
/
select YADAMU.YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID,YADAMU.YADAMU_INSTALLATION_TIMESTAMP() YADAMU_INSTALLATION_TIMESTAMP
  from SYSIBM.SYSDUMMY1
/
--
-- The following is functions are used as Decoration Contraints to provide metadata about column type
--
CREATE FUNCTION YADAMU.IS_WKT(WKT_CONTENT CLOB)
RETURNS BOOLEAN 
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
RETURN TRUE
/
CREATE FUNCTION YADAMU.IS_WKB(WKT_CONTENT BLOB)
RETURNS BOOLEAN 
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
RETURN TRUE
/
CREATE FUNCTION YADAMU.IS_GEOJSON(GEOJSON_CONTENT BLOB)
RETURNS BOOLEAN 
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
RETURN TRUE
/
CREATE FUNCTION YADAMU.IS_XML(XML_CONTENT DBCLOB)
-- Used when XML is stored as DBCLOB
RETURNS BOOLEAN 
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
RETURN TRUE
/
CREATE FUNCTION YADAMU.IS_JSON(JSON_CONTENT BLOB)
RETURNS BOOLEAN 
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
RETURN TRUE
/
CREATE OR REPLACE FUNCTION YADAMU.HEXTOBLOB (HEX_VALUE CLOB(16M))
RETURNS BLOB(16M)
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
BEGIN
  DECLARE RAW_VALUE BLOB(16M);
  
  DECLARE HEX_LENGTH BIGINT;
  DECLARE OFFSET BIGINT;

  DECLARE HEX_CHUNK VARCHAR(32672);
  DECLARE RAW_CHUNK BLOB(16336);
  
  IF (HEX_VALUE is NULL) THEN
    return NULL;
  END If;
  
  SET HEX_LENGTH = LENGTH(HEX_VALUE);
  SET OFFSET = 1;
  
  SET RAW_VALUE = EMPTY_BLOB();
  
  WHILE (OFFSET <= HEX_LENGTH) DO
    SET HEX_CHUNK = SUBSTR(HEX_VALUE,OFFSET,32672);
	SET HEX_CHUNK = TRIM(TRAILING FROM HEX_CHUNK);
    SET RAW_CHUNK  = HEXTORAW(HEX_CHUNK);
	SET OFFSET = OFFSET + LENGTH(HEX_CHUNK);
  	SET RAW_VALUE = RAW_VALUE CONCAT RAW_CHUNK;
  END WHILE;
	
  RETURN RAW_VALUE;
END;
/
CREATE OR REPLACE FUNCTION YADAMU.BLOBTOHEX(RAW_VALUE BLOB(16M))
RETURNS CLOB(16M)
DETERMINISTIC 
NO EXTERNAL ACTION 
CONTAINS SQL 
BEGIN
  DECLARE HEX_VALUE CLOB(16M);
  
  DECLARE RAW_LENGTH BIGINT;
  DECLARE OFFSET BIGINT;

  DECLARE RAW_CHUNK BLOB(16336);
  DECLARE HEX_CHUNK VARCHAR(32672);

  IF (RAW_VALUE is NULL) THEN
    return NULL;
  END If;
  
  
  SET RAW_LENGTH = LENGTH(RAW_VALUE);
  SET OFFSET = 1;
  
  SET HEX_VALUE = EMPTY_CLOB();
  
  WHILE (OFFSET <= RAW_LENGTH) DO
    SET RAW_CHUNK = SUBSTRB(RAW_VALUE,OFFSET,16336);
	SET HEX_CHUNK = TRIM(TRAILING FROM HEX_CHUNK);
    SET HEX_CHUNK  = RAWTOHEX(RAW_CHUNK);
	SET OFFSET = OFFSET + LENGTH(RAW_CHUNK);
  	SET HEX_VALUE = HEX_VALUE CONCAT HEX_CHUNK;
  END WHILE;
	
  RETURN HEX_VALUE;
END;
/