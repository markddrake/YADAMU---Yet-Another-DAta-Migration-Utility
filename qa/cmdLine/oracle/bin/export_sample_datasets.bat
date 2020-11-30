@echo %YADAMU_TRACE%
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set START_TIME=%%F
call qa\cmdLine\bin\initialize.bat %1 %~dp0 oracle export %YADAMU_TESTNAME%
rmdir /s /q %YADAMU_EXPORT_PATH%
set MODE=DDL_ONLY
set YADAMU_EXPORT_TARGET=%YADAMU_EXPORT_PATH%\%MODE%
mkdir %YADAMU_EXPORT_TARGET%
call %YADAMU_SCRIPT_PATH%\export_operations_Oracle.bat %YADAMU_EXPORT_TARGET% "" "" %MODE%
set MODE=DATA_ONLY
set YADAMU_EXPORT_TARGET=%YADAMU_EXPORT_PATH%\%MODE%
mkdir %YADAMU_EXPORT_TARGET%
call %YADAMU_SCRIPT_PATH%\export_operations_Oracle.bat %YADAMU_EXPORT_TARGET% "" "" %MODE%
set MODE=DDL_AND_DATA
set YADAMU_EXPORT_TARGET=%YADAMU_EXPORT_PATH%\%MODE%
mkdir %YADAMU_EXPORT_TARGET%
call %YADAMU_SCRIPT_PATH%\export_operations_Oracle.bat %YADAMU_EXPORT_TARGET% "" "" %MODE%for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set END_TIME=%%F
set /A ELAPSED_TIME=END_TIME-START_TIME
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "&{$ts=[timespan]::fromseconds($env:ELAPSED_TIME);$ts.ToString(\"hh\:mm\:ss\")}"`)  do set ELAPSED_TIME=%%F
echo "Export %YADAMU_DATABASE%. Elapsed time: %ELAPSED_TIME%. Log Files written to %YADAMU_LOG_PATH%."