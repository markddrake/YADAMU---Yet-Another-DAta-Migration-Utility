@set DIR=..\..\JSON\MYSQL
@set SCHEMA=sakila
@set FILENAME=sakila
call env\connection.bat
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PORT=%DB_PORT% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME% --File=%DIR%\%FILENAME%.json owner=%SCHEMA%
