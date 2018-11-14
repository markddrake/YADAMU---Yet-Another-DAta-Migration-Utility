call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHVER=1
mkdir %DIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_ORACLE_ALL.sql %LOGDIR% %SCHVER% JSON_TABLE %MODE%
call windows\import_Oracle_jTable.bat %MDIR% %SCHVER% ""
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_ORACLE_ALL.sql %LOGDIR% "" 1 JSON_TABLE %MODE%
@set SCHVER=2
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\RECREATE_ORACLE_ALL.sql %LOGDIR% %SCHVER% JSON_TABLE %MODE%
call windows\import_Oracle_jTable.bat %DIR% %SCHVER% 1
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @sql\COMPARE_ORACLE_ALL.sql %LOGDIR% 1 2 JSON_TABLE %MODE%
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false