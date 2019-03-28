FOR /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do @set START_TIME=%%F
call ..\windows\initialize.bat %~dp1
@set MODE=DATA_ONLY
call %YADAMU_SCRIPT_ROOT%\windows\clone_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\clone_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\windows\clone_MSSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_Oracle.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MSSQL.bat
@set MODE=DDL_AND_DATA
call %YADAMU_SCRIPT_ROOT%\windows\clone_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\windows\jTable_Oracle.bat 
FOR /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do @set END_TIME=%%F
@SET /A ELAPSED_TIME=END_TIME-START_TIME
@ECHO "%YADAMU_DB%: Completed. Elapsed time: %ELAPSED_TIME%s. Log Files written to %YADAMU_LOG_PATH%."