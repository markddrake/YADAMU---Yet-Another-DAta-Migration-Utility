@set DIR=JSON\\MSSQL
@set MDIR=..\\..\\JSON\\MSSQL 
@set ID=1
@set SCHEMA=ADVWRK
@set FILENAME=AdventureWorks
mkdir %DIR%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <..\sql\JSON_IMPORT.sql
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="SET @ID=%ID%" <sql\RECREATE_MSSQL_ALL.sql
call windows\import_MSSQL_jTable.bat %MDIR% %ID% ""
call windows\export_MSSQL %DIR% %ID% %ID%
@set ID=2
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="SET @ID=%ID%" <sql\RECREATE_MSSQL_ALL.sql
call windows\import_MSSQL_jTable.bat %DIR% %ID% 1
call windows\export_MSSQL %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json