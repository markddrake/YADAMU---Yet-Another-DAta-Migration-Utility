VAR JSON CLOB
--
DEF SCHEMA_NAME = &1
--
set timing on
--
begin
  select JSON_EXPORT.EXPORT_SCHEMA('&SCHEMA_NAME') 
    into :JSON 
	from dual;
end;	
/
select 1
  from DUAL
 where :JSON IS JSON
/
set timing off
--
set lines 1024
column JSON format A1024
set feedback off
set heading off
set termout off
set verify off
set long 1000000000
set pages 0
set echo off
spool JSON/&SCHEMA_NAME..json
print :JSON
spool off
set echo on
set pages 100
set verify on
set termout on
set heading on
set feedback on
