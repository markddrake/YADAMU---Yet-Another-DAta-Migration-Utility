--
set pages 100
set lines 256
column SOURCE_SCHEMA format a32
column TARGET_SCHEMA FORMAT A32
column TABLE_NAME format A48
column SQLERRM format A64
--
set feedback off
set heading off
--
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "&SCHEMA&ID1", "&SCHEMA&ID2", "&METHOD", "&MODE"' || CHR(13) "Timestamp"
  from DUAL
/
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
set heading on
set feedback on
--
select * 
  from SCHEMA_COMPARE_RESULTS 
 order by SOURCE_SCHEMA, TABLE_NAME
/ 
--