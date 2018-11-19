set echo off
set feedback off
set heading off
set verify off
--
-- Enable Empty Strings ("") as command line parameter values
--
select '' "1", '' "2", '' "3", '' "4"  from dual where rownum = 0
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
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "{MSSQL}&ID1", "{MSSQL}&ID2", "&METHOD"' || CHR(13) "Timestamp"
  from DUAL
/
def SCHEMA = Northwind
--
@@DO_SCHEMA_COMPARE
--
def SCHEMA = Sales
--
@@DO_SCHEMA_COMPARE
--
def SCHEMA = Person
--
@@DO_SCHEMA_COMPARE
--
def SCHEMA = Production
--
@@DO_SCHEMA_COMPARE
--
def SCHEMA = Purchasing
--
@@DO_SCHEMA_COMPARE
--
def SCHEMA = HumanResources
--
@@DO_SCHEMA_COMPARE
--
def SCHEMA = DW
--
@@DO_SCHEMA_COMPARE
-- 
quit

