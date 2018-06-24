set echo on
spool logs/IMPORT_SCHEMA_&3..log 
--
def JSON_DIR = &1
--
def FILENAME = &2
--
def SCHEMA = &3
--
VAR JSON CLOB
--
create or replace directory JSON_DIR as '&JSON_DIR'
/
drop user &SCHEMA cascade
/
grant connect, resource, unlimited tablespace to &SCHEMA identified by &SCHEMA
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
begin
  JSON_IMPORT.IMPORT_JSON(:JSON,'&SCHEMA');
end;
/
set pages 50 lines 256 trimspool on long 1000000
--
column TABLE_NAME format A30
column SQL_STATEMENT format A80
column STATUS format A12
column RESULT format A32
-- 
select STATUS, SQL_STATEMENT, RESULT 
  from table(JSON_EXPORT_DDL.IMPORT_DDL_LOG)
/
select TABLE_NAME, STATUS, SQL_STATEMENT, RESULT 
  from table(JSON_IMPORT.IMPORT_DML_LOG)
/
exit