echo off
(echo.whenever oserror exit failure
echo.whenever sqlerror exit failure rollback
echo.set heading off pagesize 0 feedback off linesize 400
echo.set trimout on trimspool on termout off echo off sqlprompt ''
echo.--
echo.ALTER SYSTEM SET MAX_STRING_SIZE = EXTENDED SCOPE = SPFILE
echo./
echo.shutdown immediate
echo.--
echo.startup upgrade
echo.--
echo.ALTER PLUGGABLE DATABASE ALL OPEN UPGRADE
echo./
echo.exit
) | sqlplus -s / as sysdba 
echo on
powershell -command New-Item -Force -ItemType directory %STAGE%\utl32k_cdb_pdbs_output
cd %ORACLE_HOME%/rdbms/admin
%ORACLE_HOME%\perl\bin\perl %ORACLE_HOME%\rdbms\admin\catcon.pl -u SYS/oracle -d %ORACLE_HOME%\rdbms\admin -l %STAGE%\utl32k_cdb_pdbs_output -b utl32k_cdb_pdbs_output utl32k.sql
echo off
(echo.whenever oserror exit failure
echo.whenever sqlerror exit failure rollback
echo.set heading off pagesize 0 feedback off linesize 400
echo.set trimout on trimspool on termout off echo off sqlprompt ''
echo.--
echo.shutdown immediate
echo.--
echo.startup
echo.--
echo.exit
) | sqlplus -s / as sysdba
echo on

