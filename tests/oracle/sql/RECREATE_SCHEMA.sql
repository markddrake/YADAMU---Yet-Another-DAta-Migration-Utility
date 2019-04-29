set echo on
def LOGDIR = "&1"
--
spool &LOGDIR/RECREATE_SCHEMA.log append
--
def SCHEMA = "&2"
--
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "&SCHEMA"' "Timestamp"
  from DUAL
/
drop user "&SCHEMA" cascade
/
grant connect, resource, unlimited tablespace to "&SCHEMA" identified by oracle
/
quit