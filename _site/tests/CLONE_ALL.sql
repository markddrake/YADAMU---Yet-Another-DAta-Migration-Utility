set lines 512 pages 0 long 1000000000 trimspool on
--
column STATUS format A32
column TABLE_NAME format A32
column RESULT format A64
column SQL_STATEMENT format A200
--
def SCHEMA = HR
--
@@CLONE_SCHEMA
--
def SCHEMA = SH
--
@@CLONE_SCHEMA
--
def SCHEMA = OE
--
@@CLONE_SCHEMA
--
def SCHEMA = BI
--
@@CLONE_SCHEMA
--
def SCHEMA = PM
--
@@CLONE_SCHEMA
--
def SCHEMA = IX
--
@@CLONE_SCHEMA
--
