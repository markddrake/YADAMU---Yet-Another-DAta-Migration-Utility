call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL%
@set SCHEMAVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_MSSQL_ALL.sql %LOGDIR% %SCHEMAVER% JSON_TABLE
call windows\import_MSSQL_jTable.bat %MDIR% %SCHEMAVER% ""
call windows\export_MSSQL %DIR% %SCHEMAVER% %SCHEMAVER%
@set SCHEMAVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_MSSQL_ALL.sql %LOGDIR% %SCHEMAVER% JSON_TABLE
call windows\import_MSSQL_jTable.bat %DIR% %SCHEMAVER% 1
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_MSSQL_ALL.sql %LOGDIR% 1 2 JSON_TABLE %MODE%
call windows\export_MSSQL %DIR% %SCHEMAVER% %SCHEMAVER%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false