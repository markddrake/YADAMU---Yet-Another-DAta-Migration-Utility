/*
set echo off
set feedback off
set heading off
set verify off
*/
select concat(DATE_FORMAT(CONVERT_TZ(current_timestamp,@@session.time_zone,'+00:00'),'%Y-%m-%dT%TZ'),': "{MSSQL}',@ID,'", "{MSSQL}',@ID2,'", "',@METHOD,'"') "Timestamp";
--
SET SESSION SQL_MODE=ANSI_QUOTES;
--
SET @SCHEMA = 'Northwind';
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
SET @SCHEMA = 'Sales';
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
SET @SCHEMA = 'Person';
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
SET @SCHEMA = 'Production';
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
SET @SCHEMA = 'Purchasing';
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
SET @SCHEMA = 'HumanResources';
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
SET @SCHEMA = 'DW';
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

