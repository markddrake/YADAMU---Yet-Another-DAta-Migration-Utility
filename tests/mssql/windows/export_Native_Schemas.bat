if not defined MODE set MODE=DATA_ONLY
@set YADAMU_TARGET=MsSQL
call ..\windows\initialize.bat %~dp0
if not exist %YADAMU_INPUT_PATH%\ mkdir %YADAMU_INPUT_PATH%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -dmaster -I -e -i%YADAMU_DB_ROOT%\sql\JSON_IMPORT.sql > %YADAMU_LOG_PATH%\install\JSON_IMPORT.log
call %YADAMU_SCRIPT_ROOT%\windows\export_MSSQL.bat %YADAMU_INPUT_PATH% "" ""
@set FILENAME=AdventureWorksALL
@set SCHEMA=ADVWRK
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=%SCHEMA% -i%YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_SCRIPT_ROOT%\windows\import_MSSQL_ALL.bat %YADAMU_INPUT_PATH% "" "" 
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%YADAMU_INPUT_PATH%\%FILENAME%.json owner=\"dbo\" mode=%MODE% logFile=%EXPORTLOG%