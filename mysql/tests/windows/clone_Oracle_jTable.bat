call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHVER=1
mkdir %DIR%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <..\sql\JSON_IMPORT.sql >%LOGDIR%\install\JSON_IMPORT.log
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @ID=%SCHVER%; set @METHOD='JSON_TABLE'" <sql\RECREATE_ORACLE_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle_jTable.bat %MDIR% %SCHVER% ""
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
@set SCHVER=2
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f  --init-command="set @ID=%SCHVER%; set @METHOD='JSON_TABLE'" <sql\RECREATE_ORACLE_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle_jTable.bat %DIR% %SCHVER% 1
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f  --init-command="set @ID1=1; set @ID2=%SCHVER%; set @METHOD='JSON_TABLE'" --table  <sql\COMPARE_ORACLE_ALL.sql >>%LOGDIR%/COMPARE_SCHEMA.log
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false