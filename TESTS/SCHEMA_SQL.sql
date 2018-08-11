set echo on
spool logs/EXPORT_SQL_&SCHEMA_NAME..log 
--
set pages 100 lines 256 trimspool on long 1000000
column SQL_STATEMENT format A128
select JSON_EXPORT.DUMP_SQL_STATEMENT SQL_STATEMENT
  from DUAL
/
spool off