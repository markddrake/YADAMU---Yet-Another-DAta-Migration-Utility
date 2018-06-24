set echo on
--
def SOURCE_SCHEMA = &1
--
def TARGET_SCHEMA = &2
--
spool logs/CLONE_IMPORT_SCHEMA_&TARGET_SCHEMA..log 
--
drop user &TARGET_SCHEMA cascade
/
grant connect, resource, unlimited tablespace to &TARGET_SCHEMA identified by &TARGET_SCHEMA
/
set timing on
--
begin
  JSON_IMPORT.IMPORT_JSON(:JSON,'&TARGET_SCHEMA');
end;
/
set timing off
--
set pages 50 lines 256 trimspool on long 1000000
--
select STATUS, SQL_STATEMENT, RESULT 
  from table(JSON_EXPORT_DDL.IMPORT_DDL_LOG)
/
select TABLE_NAME, STATUS, SQL_STATEMENT, RESULT 
  from table(JSON_IMPORT.IMPORT_DML_LOG)
/
--