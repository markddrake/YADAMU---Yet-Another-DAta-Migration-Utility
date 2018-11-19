call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHVER=1
mkdir %DIR%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -i..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=%SCHVER% -vMETHOD=SAX -i sql\RECREATE_ORACLE_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle.bat %MDIR% %SCHVER% ""
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
@set SCHVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=%SCHVER% -vMETHOD=SAX -i sql\RECREATE_ORACLE_ALL.sql>>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle.bat %DIR% %SCHVER% 1 
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID1=1 -vID2=%SCHVER% -vMETHOD=SAX -i sql\COMPARE_ORACLE_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE% 
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false