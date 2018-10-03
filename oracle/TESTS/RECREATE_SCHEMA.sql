set echo on
spool logs/sql/RECREATE_SCHEMA.log
--
def SCHEMA = &1
--
def ID = &2
--
drop user &SCHEMA&ID cascade
/
grant connect, resource, unlimited tablespace to &SCHEMA&ID identified by oracle
/
quit