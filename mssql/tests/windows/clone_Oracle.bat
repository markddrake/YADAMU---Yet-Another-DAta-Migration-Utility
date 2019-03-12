call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHEMAVER=1
mkdir %DIR%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -dmaster -I -e -i..\sql\JSON_IMPORT.sql > %LOGDIR%\install\JSON_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=%SCHEMAVER% -vMETHOD=Clarinet -i sql\RECREATE_ORACLE_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle.bat %MDIR% %SCHEMAVER% ""
call windows\export_Oracle.bat %DIR% %SCHEMAVER% %SCHEMAVER% %MODE%
@set SCHEMAVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=%SCHEMAVER% -vMETHOD=Clarinet -i sql\RECREATE_ORACLE_ALL.sql>>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle.bat %DIR% %SCHEMAVER% 1 
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=Clarinet -i sql\COMPARE_ORACLE_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_Oracle.bat %DIR% %SCHEMAVER% %SCHEMAVER% %MODE% 
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false