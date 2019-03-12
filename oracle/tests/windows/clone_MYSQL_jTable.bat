call env\setEnvironment.bat
@set DIR=JSON\%MYSQL%
@set MDIR=%TESTDATA%\%MYSQL%
@set SCHEMA=SAKILA
@set fileNAME=sakila
@set SCHEMAVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHEMAVER% JSON_TABLE
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%MDIR%\%fileNAME%.json toUser=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%IMPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHEMAVER% JSON_TABLE
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%1.json toUser=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%IMPORTLOG%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_SCHEMA.sql %LOGDIR% %SCHEMA% 1 2 JSON_TABLE %MODE%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false