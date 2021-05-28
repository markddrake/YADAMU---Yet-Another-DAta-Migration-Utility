@echo %YADAMU_TRACE%
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set START_TIME=%%F
call qa\cmdLine\bin\initialize.bat %~dp0 %~dp0 mysql export %YADAMU_TESTNAME%
rmdir /s /q %YADAMU_EXPORT_PATH%
mkdir %YADAMU_EXPORT_PATH%
set MODE=DATA_ONLY
set SCHEMA=jtest
set FILENAME=jsonExample
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PORT=%DB_PORT% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME% -- FILE=%YADAMU_EXPORT_PATH%\%FILENAME%.json  ENCRYPTION=false FROM_USER=\"%SCHEMA%\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
set SCHEMA=sakila
set FILENAME=sakila
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PORT=%DB_PORT% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME% -- FILE=%YADAMU_EXPORT_PATH%\%FILENAME%.json  ENCRYPTION=false FROM_USER=\"%SCHEMA%\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%%
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set END_TIME=%%F
set /A ELAPSED_TIME=END_TIME-START_TIME
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "&{$ts=[timespan]::fromseconds($env:ELAPSED_TIME);$ts.ToString(\"hh\:mm\:ss\")}"`)  do set ELAPSED_TIME=%%F
echo "Export %YADAMU_DATABASE%. Elapsed time: %ELAPSED_TIME%. Log Files written to %YADAMU_LOG_PATH%."