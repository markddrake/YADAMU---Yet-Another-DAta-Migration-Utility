@set DIR=..\..\JSON\MYSQL
@set SCHEMA=sakila
@set FILENAME=sakila
node ..\node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=%DIR%\%FILENAME%.json owner=%SCHEMA%
