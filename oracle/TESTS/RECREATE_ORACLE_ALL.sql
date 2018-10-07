set echo on
spool logs/sql/RECREATE_ORACLE_ALL.log
--
def ID = &1
--
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