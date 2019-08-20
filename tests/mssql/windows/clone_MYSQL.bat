@set YADAMU_TARGET=MySQL
@set YADAMU_PARSER=CLARINET
call ..\windows\initialize.bat %~dp0
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -dmaster -I -e -i%YADAMU_DB_ROOT%\sql\YADAMU_IMPORT.sql > %YADAMU_LOG_PATH%\install\YADAMU_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -dmaster -I -e -i%YADAMU_SCRIPT_ROOT%\sql\YADAMU_TEST.sql > %YADAMU_LOG_PATH%\install\YADAMU_TEST.log
@set FILENAME=sakila
@set SCHEMA=sakila
@set SCHEMAVER=1
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_INPUT_PATH%\%FILENAME%.json toUser=\"dbo\" mode=%MODE%  logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"dbo\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -i%YADAMU_SCRIPT_ROOT%\sql\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"dbo\" mode=%MODE% logFile=%EXPORTLOG%
@set FILENAME=jsonExample
@set SCHEMA=jtest
@set SCHEMAVER=1
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_INPUT_PATH%\%FILENAME%.json toUser=\"dbo\" mode=%MODE%  logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"dbo\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -i%YADAMU_SCRIPT_ROOT%\sql\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"dbo\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_HOME%\utilities\node/compareFileSizes %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH%
node %YADAMU_HOME%\utilities\node/compareArrayContent %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH% false