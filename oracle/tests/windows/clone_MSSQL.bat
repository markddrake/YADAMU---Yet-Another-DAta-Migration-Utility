call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL% 
@set SCHVER=1
@set SCHEMA=ADVWRK
@set fileNAME=AdventureWorks
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHVER% SAX
call windows\import_MSSQL_ALL.bat %MDIR% %SCHEMA% %SCHVER% "" 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE%  logFile=%EXPORTLOG%
@set SCHVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHVER% SAX
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%1.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_SCHEMA.sql %LOGDIR% %SCHEMA% 1 2 SAX %MODE%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE%  logFile=%EXPORTLOG%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false