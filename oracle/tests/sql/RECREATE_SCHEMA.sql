set echo on
def LOGDIR = &1
spool &LOGDIR/RECREATE_SCHEMA.log append
--
def SCHEMA = &2
--
def ID = &3
--
def METHOD = &4
--
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "&SCHEMA&ID", "&METHOD"' "Timestamp"
  from DUAL
/
drop user &SCHEMA&ID cascade
/
grant connect, resource, unlimited tablespace to &SCHEMA&ID identified by oracle
/
quit