--
-- Resize the UNDO and USER tablespaces
--
select file_id, file_name from DBA_DATA_FILES
/
COLUMN FILE_ID new_value FILE_ID
select FILE_ID 
  from DBA_DATA_FILES
 where TABLESPACE_NAME = 'USERS'
/
alter database datafile &FILE_ID resize 1G
/
alter database datafile &FILE_ID autoextend on next 256M
/
COLUMN FILE_ID new_value FILE_ID
select FILE_ID 
  from DBA_DATA_FILES
 where TABLESPACE_NAME = 'UNDOTBS1'
/
alter database datafile &FILE_ID resize 1G
/
alter database datafile &FILE_ID autoextend on next 256M
/
--
-- Grant permission required to use DBMS_CRYPTO during Schema Compare Operations
--
grant execute on DBMS_CRYPTO to SYSTEM
/
--
-- Reconfigure the Database
--
COLUMN SCRIPT_NAME new_value SCRIPT_NAME
select 'configure_' ||  SUBSTR('&_O_RELEASE',1,4) || '.sql' SCRIPT_NAME 
  from dual
/
--
@@&SCRIPT_NAME
--
alter user system identified by oracle
/
--
-- Move the Recovery File Location to the 'oradata' volume
--
alter system set  db_recovery_file_dest='/opt/oracle/oradata/flash_recovery_area' scope=spfile
/
alter system set  db_recovery_file_dest_size=4G scope=spfile
/
--
-- Move the ADR Repository to the 'oradata' volume
--
alter system SET diagnostic_dest='/opt/oracle/oradata' scope=spfile
/
--
--
-- Move the AUDIT folder to the 'oradata' volume
--
column DB_NAME new_value DB_NAME
select sys_context('USERENV','DB_NAME') DB_NAME from DUAL
/
alter system set audit_file_dest='/opt/oracle/oradata/admin/&DB_NAME/adump' scope=spfile
/
-- 
-- Resize SGA and PGA
--
alter system set SGA_MAX_SIZE = 4G scope = spfile
/
alter system set SGA_TARGET = 3G scope = spfile
/
alter system set PGA_AGGREGATE_LIMIT = 16G scope = spfile
/
alter system set PGA_AGGREGATE_TARGET = 0 scope = spfile
/
--
--
-- Shutdown to force changes to take effect
--
shutdown immediate;
connect / as sysdba
startup
alter system register
/
exit
