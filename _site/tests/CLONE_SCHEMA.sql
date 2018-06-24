spool logs/CLONE_SCHEMA_&SCHEMA..log
--
drop user &SCHEMA.1 cascade
/
grant connect, resource, unlimited tablespace to &SCHEMA.1 identified by oracle
/
begin
  select COLUMN_VALUE
    into :JSON
	from TABLE(JSON_EXPORT.EXPORT_SCHEMA('&SCHEMA'));
end;
/
@@DUMP_JSON &SCHEMA
--
set lines 512
spool logs/CLONE_SCHEMA_&SCHEMA..log APPEND
--
select SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  from DUAL
/
exec JSON_IMPORT.IMPORT_JSON(:JSON,'&SCHEMA.1');
--
select SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  from DUAL
/
select STATUS, SQL_STATEMENT, RESULT
  from TABLE(JSON_EXPORT_DDL.ERROR_LOG)
/
select TABLE_NAME, SQL_STATEMENT, RESULT
  from TABLE(JSON_IMPORT.SQL_OPERATIONS)
/
@@COMPARE_SCHEMAS &SCHEMA &SCHEMA.1
--
spool off
