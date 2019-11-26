FOR /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do @set START_TIME=%%F
for %%I in (%~dp1..) do set YADAMU_DB=%%~nxI
@set YADAMU_SCRIPT_ROOT=%~dp1
@set MODE=DATA_ONLY
call %YADAMU_SCRIPT_ROOT%\import_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\import_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\import_MSSQL.bat
if exists %YADAMU_SCRIPT_ROOT%\upload_MySQL.bat call %YADAMU_SCRIPT_ROOT%\upload_MYSQL.bat
if exists %YADAMU_SCRIPT_ROOT%\upload_Oracle.bat call %YADAMU_SCRIPT_ROOT%\upload_Oracle.bat
if exists %YADAMU_SCRIPT_ROOT%\upload__MSSQL.bat call %YADAMU_SCRIPT_ROOT%\upload_MSSQL.bat
@set MODE=DDL_AND_DATA
call %YADAMU_SCRIPT_ROOT%\import_Oracle.bat 
if exists %YADAMU_SCRIPT_ROOT%\upload__MSSQL.bat call %YADAMU_SCRIPT_ROOT%\upload_MSSQL.bat
FOR /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do @set END_TIME=%%F
@SET /A ELAPSED_TIME=END_TIME-START_TIME
@ECHO "%YADAMU_WORK_ID%: Completed. Elapsed time: %ELAPSED_TIME%s. Log Files written to %YADAMU_LOG_PATH%."