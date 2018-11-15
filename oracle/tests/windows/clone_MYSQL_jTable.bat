call env\setEnvironment.bat
@set DIR=JSON\%MYSQL%
@set MDIR=%TESTDATA%\%MYSQL%
@set SCHEMA=SAKILA
@set fileNAME=sakila
@set SCHVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHVER% JSON_TABLE
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%MDIR%\%fileNAME%.json toUser=\"%SCHEMA%%SCHVER%\"
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHVER% JSON_TABLE
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%1.json toUser=\"%SCHEMA%%SCHVER%\"
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_SCHEMA.sql %LOGDIR% %SCHEMA% 1 2 JSON_TABLE 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false