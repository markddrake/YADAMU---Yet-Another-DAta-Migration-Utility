set echo on 
spool logs/SCHEMA_JSON_ALL_SCHEMAS.log
--
--
set pages 0
set lines 256
set long 1000000000
--
column SQL_STATEMENT FORMAT A256
column JSON_DOCUMENT FORMAT A256
--
VAR JSON CLOB
--
set lines 256
def SCHEMA = HR
spool logs/SCHEMA_JSON_&SCHEMA..log APPEND
--
@@SCHEMA_JSON_SQL &SCHEMA 
--
set lines 256
def SCHEMA = SH
spool logs/SCHEMA_JSON_&SCHEMA..log APPEND
--
@@SCHEMA_JSON_SQL &SCHEMA 
--
set lines 256
def SCHEMA = OE
spool logs/SCHEMA_JSON_&SCHEMA..log APPEND
--
@@SCHEMA_JSON_SQL &SCHEMA 
--
set lines 256
def SCHEMA = PM
spool logs/SCHEMA_JSON_&SCHEMA..log APPEND
--
@@SCHEMA_JSON_SQL &SCHEMA 
--
set lines 256
def SCHEMA = IX
spool logs/SCHEMA_JSON_&SCHEMA..log APPEND
--
@@SCHEMA_JSON_SQL &SCHEMA 
--
set lines 256
def SCHEMA = BI
spool logs/SCHEMA_JSON_&SCHEMA..log APPEND
--
@@SCHEMA_JSON_SQL &SCHEMA 
--
quit

