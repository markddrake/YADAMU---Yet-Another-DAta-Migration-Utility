call env\setEnvironment.bat
@set DIR=JSON\%MYSQL%
@set MDIR=%TESTDATA%\%MYSQL%
@set SCHEMAVER=1
@set SCHEMA=SAKILA
@set FILENAME=sakila
@set SCHEMAVER=1
mkdir %DIR%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -dmaster -I -e -i..\sql\JSON_IMPORT.sql > %LOGDIR%\install\JSON_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD=JSON_TABLE -isql\RECREATE_SCHEMA.sql >>%LOGDIR%\RECREATE_SCHEMA.log
node ..\node\jTableimport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%MDIR%\%FILENAME%.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%DIR%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD='JSON_TABLE' -i sql\RECREATE_SCHEMA.sql >>%LOGDIR%\RECREATE_SCHEMA.log
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%DIR%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=JSON_TABLE -i sql\COMPARE_SCHEMA.sql >>%LOGDIR%\COMPARE_SCHEMA.log
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%DIR%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false