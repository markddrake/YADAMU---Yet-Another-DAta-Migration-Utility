--
-- Resize the UNDO and USER tablespaces
--
set echo on

def STAGE_LOCATION=&1
def ORADATA_LOCATION=&2
--
spool &STAGE_LOCATION/log/configure.log
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
-- Set Temp Tablespace to 1G
--
select FILE_ID 
  from DBA_TEMP_FILES
 where TABLESPACE_NAME = 'TEMP'
/
alter database tempfile &FILE_ID resize 1G
/
alter database tempfile &FILE_ID autoextend on next 256M
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

alter system register
/
--
-- Move the Recovery File Location to the 'oradata' volume
--
-- Changes integrated into the to dbca.rsp file 
--
-- alter system set  db_recovery_file_dest='&ORADATA_LOCATION/oradata/flash_recovery_area' scope=spfile
-- /
-- alter system set  db_recovery_file_dest_size=4G scope=spfile
-- /
--
-- Restart to force changes to take effect
--
-- shutdown immediate;
-- connect / as sysdba
-- startup
--
-- Verify
--
show PARAMETER processes
/
exit
