set echo on
--
select '' "1", '' "2", '' "3"  from dual where rownum = 0
/
def LOGDIR = "&1"
--
spool &LOGDIR/RECREATE_SCHEMA.log append
--
def ID = "&2"
--
def METHOD = "&3"
--
select to_char(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': "&METHOD"' "Timestamp"
  from DUAL
/
drop user "Northwind&ID" cascade
/
grant connect, resource, unlimited tablespace to "Northwind&ID" identified by oracle
/
drop user "Sales&ID" cascade
/
grant connect, resource, unlimited tablespace to "Sales&ID" identified by oracle
/
drop user "Person&ID" cascade
/
grant connect, resource, unlimited tablespace to "Person&ID" identified by oracle
/
drop user "Production&ID" cascade
/
grant connect, resource, unlimited tablespace to "Production&ID" identified by oracle
/
drop user "Purchasing&ID" cascade
/
grant connect, resource, unlimited tablespace to "Purchasing&ID" identified by oracle
/
drop user "HumanResources&ID" cascade
/
grant connect, resource, unlimited tablespace to "HumanResources&ID" identified by oracle
/
drop user "DW&ID" cascade
/
grant connect, resource, unlimited tablespace to "DW&ID" identified by oracle
/
quit