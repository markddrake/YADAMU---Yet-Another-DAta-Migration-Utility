--
-- Resize the UNDO and USER tablespaces
--
set echo on
--
spool /opt/oracle/diag/setup/configure.log
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
-- Move the Recovery File Location to the 'diag' volume
--
alter system set  db_recovery_file_dest='/opt/oracle/oradata/flash_recovery_area' scope=spfile
/
alter system set  db_recovery_file_dest_size=4G scope=spfile
/
--
-- Restart to force changes to take effect
--
shutdown immediate;
connect / as sysdba
startup
alter system register
/
exit
