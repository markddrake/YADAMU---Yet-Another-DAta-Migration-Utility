set echo off
set feedback off
set heading off
set verify off
--
-- Enable Empty Strings ("") as command line parameter values
--
select '' "1", '' "2", '' "3", '' "4", '' "5", '' "6" from dual where rownum = 0
/
def LOGDIR ="&1"
--
spool &LOGDIR/COMPARE_SCHEMA.log append
--
def SCHEMA = "&2"
--
def ID1 = "&3"
--
def ID2 = "&4"
--
def METHOD = "&5"
--
def MODE = "&6"
--
@@SCHEMA_COMPARE.sql
--
quit