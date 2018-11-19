call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL%
@set SCHVER=1
mkdir %DIR%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -i..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=%SCHVER% -vMETHOD=SAX -isql\RECREATE_MSSQL_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_MSSQL_jTable.bat %MDIR% %SCHVER% ""
call windows\export_MSSQL %DIR% %SCHVER% %SCHVER%
@set SCHVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=%SCHVER% -vMETHOD=SAX -isql\RECREATE_MSSQL_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_MSSQL_jTable.bat %DIR% %SCHVER% 1
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID1=1 -vID2=2-vMETHOD=SAX -i sql\COMPARE_MSSQL_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_MSSQL %DIR% %SCHVER% %SCHVER%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false