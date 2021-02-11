@echo %YADAMU_TRACE%
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set START_TIME=%%F
call qa\cmdLine\bin\initialize.bat %1 %~dp0 mssql export %YADAMU_TESTNAME%
rmdir /s /q %YADAMU_EXPORT_PATH%
mkdir %YADAMU_EXPORT_PATH%
set MODE=DATA_ONLY
call %YADAMU_SCRIPT_PATH%\export_operations_MsSQL.bat %YADAMU_EXPORT_PATH% "" "" %MODE%
set FILENAME=AdventureWorksALL
set SCHEMA=ADVWRK
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_SCRIPT_PATH%\import_operations_ADVWRK.bat %YADAMU_EXPORT_PATH% "" "" 
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%SCHEMA%  FILE=%YADAMU_EXPORT_PATH%\%FILENAME%.json  FROM_USER=\"dbo\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%%
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set END_TIME=%%F
set /A ELAPSED_TIME=END_TIME-START_TIME
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "&{$ts=[timespan]::fromseconds($env:ELAPSED_TIME);$ts.ToString(\"hh\:mm\:ss\")}"`)  do set ELAPSED_TIME=%%F
echo "Export %YADAMU_DATABASE%. Elapsed time: %ELAPSED_TIME%. Log Files written to %YADAMU_LOG_PATH%."