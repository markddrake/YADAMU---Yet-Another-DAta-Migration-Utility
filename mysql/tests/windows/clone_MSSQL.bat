call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL% 
@set SCHEMAVER=1
@set SCHEMA=ADVWRK
@set FILENAME=AdventureWorks
mkdir %DIR%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <..\sql\JSON_IMPORT.sql >%LOGDIR%\install\JSON_IMPORT.log
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @SCHEMA='%SCHEMA%'; set @ID=%SCHEMAVER%; set @METHOD='Clarinet'" <sql\RECREATE_SCHEMA.sql >>%LOGDIR%\RECREATE_SCHEMA.log  
call windows\import_MSSQL_ALL.bat %MDIR% %SCHEMA% %SCHEMAVER% "" 
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME%  file=%DIR%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE%  logFile=%EXPORTLOG%
@set SCHEMAVER=2
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @SCHEMA='%SCHEMA%'; set @ID=%SCHEMAVER%; set @METHOD='Clarinet'" <sql\RECREATE_SCHEMA.sql >>%LOGDIR%\RECREATE_SCHEMA.log
node ..\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME%  file=%DIR%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% --init-command="set @SCHEMA='%SCHEMA%'; set @ID1=1; set @ID2=%SCHEMAVER%; set @METHOD='Clarinet'" --table  <sql\COMPARE_SCHEMA.sql >>%LOGDIR%\COMPARE_SCHEMA.log 
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME%  file=%DIR%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE%  logFile=%EXPORTLOG%
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false