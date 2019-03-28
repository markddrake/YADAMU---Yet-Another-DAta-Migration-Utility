if not defined MODE set MODE=DATA_ONLY
for %%I in (.) do set YADAMU_TARGET=%%~nxI
call ..\windows\initialize.bat %~dp0
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_DB_ROOT%\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%
@set MODE=DDL_ONLY
@set  YADAMU_OUTPUT_PATH=%YADAMU_INPUT_PATH%\%MODE%
if not exist %YADAMU_OUTPUT_PATH%\ mkdir %YADAMU_OUTPUT_PATH%
call %YADAMU_SCRIPT_ROOT%\windows\export_Oracle.bat %YADAMU_OUTPUT_PATH% "" "" %MODE%
@set MODE=DATA_ONLY
@set  YADAMU_OUTPUT_PATH=%YADAMU_INPUT_PATH%\%MODE%
if not exist %YADAMU_OUTPUT_PATH%\ mkdir %YADAMU_OUTPUT_PATH%
call %YADAMU_SCRIPT_ROOT%\windows\export_Oracle.bat %YADAMU_OUTPUT_PATH% "" "" %MODE%
@set MODE=DDL_AND_DATA
@set  YADAMU_OUTPUT_PATH=%YADAMU_INPUT_PATH%\%MODE%
if not exist %YADAMU_OUTPUT_PATH%\ mkdir %YADAMU_OUTPUT_PATH%
call %YADAMU_SCRIPT_ROOT%\windows\export_Oracle.bat %YADAMU_OUTPUT_PATH% "" "" %MODE%