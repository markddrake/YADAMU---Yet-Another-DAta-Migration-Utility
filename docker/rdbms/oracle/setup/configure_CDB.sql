-- CDB Specific Configuration
--
ALTER SESSION SET CONTAINER = CDB$ROOT
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