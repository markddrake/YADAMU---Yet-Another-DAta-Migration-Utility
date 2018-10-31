@set DIR=JSON\MSSQL
@set MDIR=..\JSON\MSSQL 
@set ID=1
@set FILENAME=AdventureWorks
@set SCHEMA=ADVWRK
@set ID=1
mkdir %DIR%
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=%SCHEMA% -v ID=%ID%
call scripts\import_MSSQL_jTable.bat %MDIR% %SCHEMA%%ID%
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@set ID=2
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=%SCHEMA% -v ID=%ID%
node node\jTableImport --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle --File=%DIR%\%FILENAME%1.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json