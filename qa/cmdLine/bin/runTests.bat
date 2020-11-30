@if not defined YADAMU_TRACE (set YADAMU_TRACE=off)
@REM @echo "%YADAMU_TRACE%"
@echo %YADAMU_TRACE%
if ["%~1"]==[""] (set YADAMU_SETTINGS=default) else (set YADAMU_SETTINGS=%1)
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set SUITE_START_TIME=%%F
set YADAMU_HOME=%CD%
set YADAMU_QA_HOME=%YADAMU_HOME%\qa
set YADAMU_TIMESTAMP=
set YADAMU_SCRIPT_DIR=%~dp0
call %YADAMU_SCRIPT_DIR%..\SETTINGS\%YADAMU_SETTINGS%.bat
call %YADAMU_SCRIPT_DIR%runCmdLineTests.bat
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set END_TIME=%%F
set /A ELAPSED_TIME=END_TIME-SUITE_START_TIME
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "&{$ts=[timespan]::fromseconds($env:ELAPSED_TIME);$ts.ToString(\"hh\:mm\:ss\")}"`)  do set ELAPSED_TIME=%%F
echo "Test Suite Elapsed time: %ELAPSED_TIME%."
