@set DIR=JSON\\MSSQL
@set MDIR=..\\..\\JSON\\MSSQL 
@set ID=1
@set SCHEMA=ADVWRK
@set FILENAME=AdventureWorks
mkdir %DIR%
call env\connection.bat
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <..\sql\JSON_IMPORT.sql >>%TESTSQL%\JSON_IMPORT.log
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="SET @SCHEMA='%SCHEMA%'; SET @ID=%ID%" <sql\RECREATE_SCHEMA.sql  >> %TESTLOG%\RECREATE_SCHEMA.log
call windows\import_MSSQL_ALL_jTable.bat %MDIR% %SCHEMA% %ID% "" 
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PORT=%DB_PORT% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME% --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@set ID=2
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="SET @SCHEMA='%SCHEMA%'; SET @ID=%ID%" <sql\RECREATE_SCHEMA.sql  >> %TESTLOG%\RECREATE_SCHEMA.log
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PORT=%DB_PORT% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME% --File=%DIR%\\%FILENAME%1.json toUser=%SCHEMA%%ID%
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PORT=%DB_PORT% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME% --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
node ..\..\utilities\compareFileSizes %LOGFILE% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGFILE% %MDIR% %DIR% false