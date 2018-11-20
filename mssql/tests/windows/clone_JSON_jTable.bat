call env\setEnvironment.bat
@set DIR=JSON\%JSON%
@set MDIR=%TESTDATA%\%JSON&
@set SCHEMA=JTEST
@set FILENAME=testcase
@set SCHVER=1
mkdir %DIR%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -i..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=%SCHEMA% -vID=%SCHVER% -vMETHOD=JSON_TABLE -isql\RECREATE_SCHEMA.sql >> %LOGDIR%\RECREATE_SCHEMA.log
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%MDIR%\%FILENAME%.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%DIR%\%FILENAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=%SCHEMA% -vID=%SCHVER% -vMETHOD=JSON_TABLE -isql\RECREATE_SCHEMA.sql >> %LOGDIR%\RECREATE_SCHEMA.log
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%DIR%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHVER% -vMETHOD=JSON_TABLE -i sql\COMPARE_SCHEMA.sql >> %LOGDIR%\COMPARE_SCHEMA.log 
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%DIR%\%FILENAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false