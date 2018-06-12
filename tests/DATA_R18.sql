set echo on
spool logs/DATA_R18.log
set pages 0 lines 256 long 100000000
--
exec JSON_EXPORT.DATA_ONLY_MODE(true);
--
exec JSON_EXPORT.DDL_ONLY_MODE(false);
--
exec JSON_IMPORT.DATA_ONLY_MODE(true);
--
exec JSON_IMPORT.DDL_ONLY_MODE(false);
--
VAR JSON CLOB
--
def SCHEMA = HR
--
begin
  select COLUMN_VALUE
    into :JSON
	from TABLE(JSON_EXPORT.EXPORT_SCHEMA('&SCHEMA'));
end;
/
@@DUMP_JSON &SCHEMA
--
column SQL_STATEMENT format A256
--
set pages 0 lines 256 long 100000000 trimspool on
--
select JSON_EXPORT.DUMP_SQL_STATEMENT
  from DUAL
/
