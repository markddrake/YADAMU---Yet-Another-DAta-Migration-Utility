@set DIR=JSON\MYSQL
@set MDIR=..\JSON\MYSQL
@set ID=1
@set FILENAME=sakila
@set SCHEMA=SAKILA
@set ID=1
mkdir %DIR%
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=%SCHEMA% -v ID=%ID%
node node\jSaxImport --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle  --File=%MDIR%\%FILENAME%.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@set ID=2
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=%SCHEMA% -v ID=%ID%
node node\jSaxImport --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle --File=%DIR%\%FILENAME%1.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json
