set echo off
set feedback off
set heading off
set verify off
--
-- Enable Empty Strings ("") as command line parameter values
--
select '', "1", '' "2", '' "3", '' "4", '' "5"  from dual where rownum = 0
/
def LOGDIR = "&1"
--
spool &LOGDIR/COMPARE_SCHEMA.log append
--
def ID1 = "&2"
--
def ID2 = "&3"
--
def METHOD = "&4"
--
def MODE = "&5"
--
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "{ORACLE}&ID1", "{ORACLE}&ID2", "&METHOD", "&MODE"' || CHR(13) "Timestamp"
  from DUAL
/
def SCHEMA = HR
--
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
def SCHEMA = SH
--
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
def SCHEMA = OE
--
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
def SCHEMA = PM
--
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
def SCHEMA = IX
--
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
def SCHEMA = BI
--
call JSON_IMPORT.COMPARE_SCHEMAS('&SCHEMA&ID1','&SCHEMA&ID2');
--
set pages 100
set lines 256
set heading on
set feedback on
column SOURCE_SCHEMA format a32
column TARGET_SCHEMA FORMAT A32
column TABLE_NAME format A64
--
select * 
  from SCHEMA_COMPARE_RESULTS 
 order by SOURCE_SCHEMA, TABLE_NAME
/ 
-- 
quit

