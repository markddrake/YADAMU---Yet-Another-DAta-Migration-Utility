set termout off
set echo off
set feedback off
set heading off
set verify off
--
-- Enable Empty Strings ("") as command line parameter values
--
select '' "1", '' "2", '' "3", '' "4", '' "5"  from dual where rownum = 0
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
def SCHEMA = HR
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = SH
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = OE
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = PM
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = IX
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = BI
--
@@SCHEMA_COMPARE.sql
--
quit