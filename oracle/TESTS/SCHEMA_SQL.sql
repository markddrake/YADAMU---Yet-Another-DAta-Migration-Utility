set echo on
spool logs/sql/EXPORT_SQL_&SCHEMA_NAME..log 
--
set pages 100 lines 256 trimspool on long 1000000
--
column SQL_STATEMENT format A256
--
VAR SQL_STATEMENT CLOB
--
begin
  :SQL_STATEMENT := JSON_EXPORT.DUMP_SQL_STATEMENT(); 
end;
/
print :SQL_STATEMENT
--
spool off