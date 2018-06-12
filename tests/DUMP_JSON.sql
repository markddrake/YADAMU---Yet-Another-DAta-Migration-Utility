DEF DUMP_FILENAME = &1
set lines 1024
column JSON format A1024
set heading off
set termout off 
set echo off
spool JSON/&DUMP_FILENAME..json
print :JSON 
spool off
set echo on
set termout on
set heading on

