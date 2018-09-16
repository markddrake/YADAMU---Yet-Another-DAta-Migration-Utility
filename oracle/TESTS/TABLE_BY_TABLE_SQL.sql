set echo on
spool logs/sql/EXPORT_SQL_&SCHEMA_NAME..log 
--
set pages 100 lines 256 trimspool on long 1000000
column SQL_STATEMENT format A128
select SQL_STATEMENT
  from TABLE(JSON_EXPORT.EXPORT_METADATA)
/
spool off