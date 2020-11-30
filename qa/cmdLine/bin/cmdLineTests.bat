for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set START_TIME=%%F
for %%I in (%~dp1..) do set YADAMU_VENDOR=%%~nxI
set YADAMU_SCRIPT_ROOT=%~dp1
set MODE=DATA_ONLY
call %YADAMU_SCRIPT_ROOT%\import_MySQL.bat
call %YADAMU_SCRIPT_ROOT%\import_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\import_MsSQL.bat
if exist %YADAMU_SCRIPT_ROOT%\upload_MsSQL.bat call %YADAMU_SCRIPT_ROOT%\upload_MsSQL.bat
if exist %YADAMU_SCRIPT_ROOT%\upload_MySQL.bat call %YADAMU_SCRIPT_ROOT%\upload_MySQL.bat
if exist %YADAMU_SCRIPT_ROOT%\upload_Oracle.bat call %YADAMU_SCRIPT_ROOT%\upload_Oracle.bat
set MODE=DDL_AND_DATA
call %YADAMU_SCRIPT_ROOT%\import_Oracle.bat 
if exist %YADAMU_SCRIPT_ROOT%\upload_Oracle.bat call %YADAMU_SCRIPT_ROOT%\upload_Oracle.bat
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "(New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date)).TotalSeconds"`)  do set END_TIME=%%F
set /A ELAPSED_TIME=END_TIME-START_TIME
for /F "tokens=* USEBACKQ" %%F IN (`powershell -command "&{$ts=[timespan]::fromseconds($env:ELAPSED_TIME);$ts.ToString(\"hh\:mm\:ss\")}"`)  do set ELAPSED_TIME=%%F
echo "%YADAMU_DATABASE%. Elapsed time: %ELAPSED_TIME%. Log Files written to %YADAMU_LOG_PATH%."