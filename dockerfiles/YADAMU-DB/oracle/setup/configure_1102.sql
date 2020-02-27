-- Version 11.2.0 Specific Configuration
--
/*
**
** Some sites claim this improces performance.
**
** ALTER SYSTEM SET FILESYSTEMIO_OPTIONS=ASYNCH SCOPE=SPFILE
** /
** ALTER SYSTEM SET disk_asynch_io=TRUE SCOPE=SPFILE
** /
**
*/
--
@/opt/oracle/diag/setup/resizeLogGroups.sql
--