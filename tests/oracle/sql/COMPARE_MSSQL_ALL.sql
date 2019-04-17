set echo off
set feedback off
set heading off
set verify off
--
-- Enable Empty Strings ("") as command line parameter values
--
select '' "1", '' "2", '' "3", '' "4", '' "5" from dual where rownum = 0
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
def SCHEMA = Northwind
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = Sales
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = Person
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = Production
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = Purchasing
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = HumanResources
--
@@SCHEMA_COMPARE.sql
--
def SCHEMA = AdventureWorksDW
--
@@SCHEMA_COMPARE.sql
-- 
quit

