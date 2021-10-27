set STAGE=c:\oracle\oradata\stage
cd %STAGE%
powershell -command New-Item -Force -ItemType directory -Path log
powershell -command (Get-Service  ("""OracleService""" + $env:ORACLE_SID)).waitForStatus("""Running""","""00:03:00""");
sqlplus sys/oracle@localhost:1521/%ORACLE_PDB% as sysdba @%STAGE%\setup\configure.sql %STAGE% c:\oracle
echo "REM Do Nothing" > extendedStringSizeAction.bat
echo off
(echo.whenever oserror exit failure
echo.whenever sqlerror exit failure rollback
echo.set heading off pagesize 0 feedback off linesize 400
echo.set trimout on trimspool on termout off echo off sqlprompt ''
echo.ALTER SESSION SET CONTAINER = CDB$ROOT
echo./
echo.VAR UTL32K_ACTION VARCHAR2(1024^^^)
echo.--
echo.begin
echo. select case 
echo.           when VALUE = 'EXTENDED' then
echo.             'REM MAX_STRNG_SIZE = EXTENDED'
echo.	       else
echo.		     'call %STAGE%\setup\setMaxStringSizeExtended.bat'
echo.         end SET_32K_SCRIPT 
echo.    into :UTL32K_ACTION
echo.    from V$PARAMETER
echo.   where NAME = 'max_string_size';
echo.exception
echo.  when NO_DATA_FOUND then
echo.    :UTL32K_ACTION := 'REM MAX_STRNG_SIZE NOT SUPPORTED';
echo.  when others then 
echo.    RAISE;
echo.end;
echo./
echo.spool extendedStringSizeAction.bat
echo.select :UTL32K_ACTION from dual;
echo.spool off
echo.exit
) | sqlplus -s / as sysdba 
echo on
call extendedStringSizeAction.bat
cd %stage%
echo "REM Do Nothing" > sampleSchemaAction.bat
echo off
(echo.whenever oserror exit failure
echo.whenever sqlerror exit failure rollback
echo.set heading off pagesize 0 feedback off linesize 400
echo.set trimout on trimspool on termout off echo off sqlprompt ''
echo.VAR INSTALL_ACTION VARCHAR2(1024^^^)
echo.--
echo.begin
echo.  select '%STAGE%\setup\installSampleSchemas.bat' 
echo.    into :INSTALL_ACTION
echo.    from (
echo.	   select count(*^^^) CNT 
echo.	     from ALL_USERS 
echo.	    where USERNAME in ('HR','SH','OE','PM','IX','BI'^^^)
echo.	 ^^^); 
echo.   where CNT < 6;
echo.exception
echo.  when NO_DATA_FOUND then
echo.    :INSTALL_ACTION := 'REM SAMPLE SCHEMAS ALREADY INSTALLED';
echo.  when others then 
echo.    RAISE;
echo.end;
echo./
echo.spool sampleSchemaAction.bat
echo.select :INSTALL_ACTION from dual;
echo.spool off
echo.exit
) | sqlplus -s sys/oracle@localhost:1521/%ORACLE_PDB% as sysdba
echo on
call sampleSchemaAction.bat
cd %stage%
echo "REM Do Nothing" > onlineMediaAction.bat
echo off
(echo.whenever oserror exit failure
echo.whenever sqlerror exit failure rollback
echo.set heading off pagesize 0 feedback off linesize 400
echo.set trimout on trimspool on termout off echo off sqlprompt ''
echo.VAR INSTALL_ACTION VARCHAR2(1024^^^)
echo.--
echo.begin
echo.  select 'imp system/oracle@localhost:1521/%ORACLE_PDB% file=%STAGE%\testdata\pm_online_media.exp fromuser=PM touser=PM DATA_ONLY=y' 
echo.	into :INSTALL_ACTION
echo.    from (
echo.	  select count(*^^^) CNT
echo.        from PM.ONLINE_MEDIA 
echo.	^^^)
echo.  where CNT = 0;	
echo.exception
echo.  when NO_DATA_FOUND then
echo.   	:INSTALL_ACTION := 'REM PM.ONLINE_MEDIA ALREADY INSTALLED';
echo.  when others then 
echo.    RAISE;
echo.end;
echo./
echo.spool onlineMediaAction.bat
echo.select :INSTALL_ACTION from dual;
echo.spool off
echo.exit
) | sqlplus -s sys/oracle@localhost:1521/%ORACLE_PDB% as sysdba
echo on
call onlineMediaAction.bat
sqlplus system/oracle@localhost:1521/%ORACLE_PDB% @%STAGE%\sql\COMPILE_ALL.sql %STAGE%\log
sqlplus system/oracle@localhost:1521/%ORACLE_PDB% @%STAGE%\sql\YADAMU_TEST.sql %STAGE%\log OFF
 