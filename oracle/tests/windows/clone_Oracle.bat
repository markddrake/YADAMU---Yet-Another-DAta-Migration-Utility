call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHEMAVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_ORACLE_ALL.sql %LOGDIR% %SCHEMAVER% Clarinet %MODE%
call windows\import_Oracle.bat %MDIR% %SCHEMAVER% ""
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_ORACLE_ALL.sql %LOGDIR% "" 1 Clarinet %MODE%
call windows\export_Oracle.bat %DIR% %SCHEMAVER% %SCHEMAVER% %MODE%
@set SCHEMAVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_ORACLE_ALL.sql %LOGDIR% %SCHEMAVER% Clarinet %MODE%
call windows\import_Oracle.bat %DIR% %SCHEMAVER% 1 
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_ORACLE_ALL.sql %LOGDIR% 1 2 Clarinet %MODE%
call windows\export_Oracle.bat %DIR% %SCHEMAVER% %SCHEMAVER% %MODE% 
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false