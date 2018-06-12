--
spool logs/&SCHEMA..log
--
declare
  V_RESULT BOOLEAN;
  V_TEST_FOLDER VARCHAR2(2000) := :OUTPUT_PATH;
  V_OUTPUT_PATH VARCHAR2(2000) := :OUTPUT_PATH || '/&SCHEMA..json';
  
  
  V_JSON_DOCUMENT CLOB;
  
  cursor getSQLStatements 
  is
  select * 
    from TABLE(JSON_EXPORT.EXPORT_METADATA); 
begin
  select COLUMN_VALUE
    into V_JSON_DOCUMENT
	from TABLE(JSON_EXPORT.EXPORT_SCHEMA('&SCHEMA'));
  V_RESULT := DBMS_XDB.createResource(V_OUTPUT_PATH,V_JSON_DOCUMENT);
  commit;
  :JSON_DOCUMENT := V_JSON_DOCUMENT;
  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN   
    V_OUTPUT_PATH := V_TEST_FOLDER || '/&SCHEMA..sql';
    V_RESULT := DBMS_XDB.createResource(V_OUTPUT_PATH,JSON_EXPORT.DUMP_SQL_STATEMENT);
  $ELSE
  for t in getSQLStatements loop
    V_OUTPUT_PATH := V_TEST_FOLDER || '/&SCHEMA..' || t.TABLE_NAME || '.sql';
    V_RESULT := DBMS_XDB.createResource(V_OUTPUT_PATH,t.SQL_STATEMENT);
  end loop;
  $END
  commit;
end;
/
select ANY_PATH
  from RESOURCE_VIEW
 where under_path(RES,:OUTPUT_PATH) = 1
/
set termout off
set echo off
set timing off
set feedback off
spool logs/&SCHEMA..json
PRINT :JSON_DOCUMENT
spool off
set termout on
set echo on
set timing on
set feedback on
--
