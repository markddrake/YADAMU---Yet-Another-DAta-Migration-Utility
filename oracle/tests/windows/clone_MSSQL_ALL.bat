call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL%
@set SCHVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_MSSQL_ALL.sql %LOGDIR% %SCHVER% SAX
call windows\import_MSSQL.bat %MDIR% %SCHVER% ""
call windows\export_MSSQL %DIR% %SCHVER% %SCHVER%
@set SCHVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_MSSQL_ALL.sql %LOGDIR% %SCHVER% SAX
call windows\import_MSSQL.bat %DIR% %SCHVER% 1
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_MSSQL_ALL.sql %LOGDIR% 1 2 SAX %MODE%
call windows\export_MSSQL %DIR% %SCHVER% %SCHVER%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false