set echo on
spool logs/CLONE_ALL_SCHEMAS.log
--
var JSON CLOB
--
column DUMP format A256
--
set pages 0 lines 256 long 100000000
--
exec JSON_EXPORT.DATA_ONLY_MODE(false);
--
exec JSON_EXPORT.DDL_ONLY_MODE(false);
--
exec JSON_IMPORT.DATA_ONLY_MODE(false);
--
exec JSON_IMPORT.DDL_ONLY_MODE(false);
--
@@CLONE_ALL.sql
--
