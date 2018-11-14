call env\setEnvironment.bat.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHVER=1
mkdir %DIR%
call envcall.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_ORACLE_ALL.sql %LOGDIR% %SCHVER% SAX %MODE%
call windows\import_Oracle.bat %MDIR% %SCHVER% ""
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_ORACLE_ALL.sql %LOGDIR% \"''\" 1 SAX %MODE%
@set SCHVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_ORACLE_ALL.sql %LOGDIR% %SCHVER% SAX %MODE%
call windows\import_Oracle.bat %DIR% %SCHVER% 1 
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_ORACLE_ALL.sql %LOGDIR% 1 2 SAX %MODE%
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE% 
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false