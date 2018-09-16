set echo on
spool logs/sql/TEST_EXPORT_FILE.log
--
def JSON_DIR = &1
--
def FILENAME = &2
--
VAR JSON CLOB
--
create or replace directory JSON_DIR as '&JSON_DIR'
/
DECLARE
  V_DEST_OFFSET NUMBER := 1;
  V_SRC_OFFSET  NUMBER := 1;
  V_CONTEXT     NUMBER := 0;
  V_WARNINGS    NUMBER := 0;
  V_BFILE	     BFILE := BFILENAME('JSON_DIR','&FILENAME');
begin
  DBMS_LOB.createTemporary(:JSON,TRUE,DBMS_LOB.SESSION);
  DBMS_LOB.FILEOPEN(V_BFILE,DBMS_LOB.FILE_READONLY);
  DBMS_LOB.LOADCLOBFROMFILE (:JSON,V_BFILE,DBMS_LOB.LOBMAXSIZE,V_DEST_OFFSET,V_SRC_OFFSET,NLS_CHARSET_ID('AL32UTF8'),V_CONTEXT,V_WARNINGS);
  DBMS_LOB.FILECLOSE(V_BFILE);
end;
/
select 1
	from DUAL
  where :JSON IS JSON
/
 select OWNER
        ,TABLE_NAME
        ,'insert ' ||
		 case 
		   when DESERIALIZATION_FUNCTIONS is NULL
		     then ''
			 else ' /*+ WITH_PLSQL */ '
		 end ||
	    'into "' || TABLE_NAME ||'"(' || SELECT_LIST || ')' || CHR(13) ||
		 case 
		   when DESERIALIZATION_FUNCTIONS is NULL 
		   then to_clob('')
		   else 'with ' || JSON_IMPORT.GENERATE_DESERIALIZATION_FUNTIONS(DESERIALIZATION_FUNCTIONS,OWNER,TABLE_NAME)
		 end ||	  
		 'select ' || INSERT_SELECT_LIST || CHR(13) ||
		 '  from JSON_TABLE(' || CHR(13) ||
	     '         :JSON,' || CHR(13) ||
		 '         ''$.data."' || TABLE_NAME || '"[*]''' || CHR(13) ||
		 '         COLUMNS(' || CHR(13) ||  COLUMN_PATTERNS || CHR(13) || '))' 
	    ,NULL
	    ,NULL
		,NULL
    from JSON_TABLE(
	        :JSON,
			'$.metadata.*' ERROR ON ERROR
			COLUMNS (
			  OWNER                        VARCHAR2(128) PATH '$.owner'
			, TABLE_NAME                   VARCHAR2(128) PATH '$.tableName'
			,  SELECT_LIST               VARCHAR2(32767) PATH '$.columns'
			,  INSERT_SELECT_LIST        VARCHAR2(32767) PATH '$.insertSelectList'
			,  COLUMN_PATTERNS           VARCHAR2(32767) PATH '$.columnPatterns'
			,  DESERIALIZATION_FUNCTIONS  VARCHAR2(4000) PATH '$.deserializationFunctions'
			)
		  );