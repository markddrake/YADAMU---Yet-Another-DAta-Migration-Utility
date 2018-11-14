set echo on
def LOGDIR = &1
spool &LOGDIR/RECREATE_SCHEMA.log append
--
def ID = &2
--
def METHOD = &3
--
def MODE = &4
--
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "ORACLE", "&METHOD", "&MODE"' "Timestamp"
  from DUAL
/
drop user HR&ID cascade
/
grant connect, resource, unlimited tablespace to HR&ID identified by oracle
/
drop user SH&ID cascade
/
grant connect, resource, unlimited tablespace to SH&ID identified by oracle
/
drop user OE&ID cascade
/
grant connect, resource, unlimited tablespace to OE&ID identified by oracle
/
drop user PM&ID cascade
/
grant connect, resource, unlimited tablespace to PM&ID identified by oracle
/
drop user IX&ID cascade
/
grant connect, resource, unlimited tablespace to IX&ID identified by oracle
/
drop user BI&ID cascade
/
grant connect, resource, unlimited tablespace to BI&ID identified by oracle
/
quit