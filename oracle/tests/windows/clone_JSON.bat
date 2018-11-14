call env\setEnvironment.bat
@set DIR=JSON\%JSON%
@set MDIR=%TESTDATA%\%JSON&
@set SCHEMA=JTEST
@set fileNAME=testcase
@set SCHVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHVER% SAX
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%MDIR%\%fileNAME%.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE%  logFile=%EXPORTLOG%
@set SCHVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_SCHEMA.sql %LOGDIR% %SCHEMA% %SCHVER% SAX
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%1.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION%  file=%DIR%\%fileNAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE%  logFile=%EXPORTLOG%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_SCHEMA.sql %LOGDIR% %SCHEMA% 1 2 SAX \"''\"
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false