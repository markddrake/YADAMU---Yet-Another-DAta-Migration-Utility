set echo on
spool createSchemas.log
--
drop user HR2 cascade
/
grant connect, resource, unlimited tablespace to HR2 identified by oracle
/
drop user SH2 cascade
/
grant connect, resource, unlimited tablespace to SH2 identified by oracle
/
drop user OE2 cascade
/
grant connect, resource, unlimited tablespace to OE2 identified by oracle
/
drop user PM2 cascade
/
grant connect, resource, unlimited tablespace to PM2 identified by oracle
/
drop user IX2 cascade
/
grant connect, resource, unlimited tablespace to IX2 identified by oracle
/
drop user BI2 cascade
/
grant connect, resource, unlimited tablespace to BI2 identified by oracle
/
quit