/*
set echo off
set feedback off
set heading off
set verify off
*/
select concat(DATE_FORMAT(CONVERT_TZ(current_timestamp,@@session.time_zone,'+00:00'),'%Y-%m-%dT%TZ'),': "',@SCHEMA,@ID1,'", "',@SCHEMA,@ID2,'", "',@METHOD,'"') "Timestamp";
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
/*
set pages 100
set lines 256
set heading on
set feedback on
column SOURCE_SCHEMA format a32
column TARGET_SCHEMA FORMAT A32
column TABLE_NAME format A64
*/
--
select * 
  from SCHEMA_COMPARE_RESULTS 
 order by SOURCE_SCHEMA, TABLE_NAME;
--
quit