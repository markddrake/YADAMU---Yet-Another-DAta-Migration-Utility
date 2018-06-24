set echo on
DEF SCHEMA = PM
spool logs/TABLE_JSON_SQL_&SCHEMA..log
--
set pages 0
set lines 256
set long 1000000000
--
column SQL_STATEMENT FORMAT A256
column JSON_DOCUMENT FORMAT A256
--
VAR JSON CLOB
--
spool logs/TABLE_JSON_SQL_&SCHEMA..log APPEND
--
DEF TABLE = PRINT_MEDIA
--
@@TABLE_SQL_JSON
--
set lines 256
spool logs/TABLE_JSON_SQL_&SCHEMA..log APPEND
--
DEF TABLE = ONLINE_MEDIA
--
@@TABLE_SQL_JSON
--
set lines 256
spool logs/TABLE_JSON_SQL_&SCHEMA..log APPEND
--
quit

