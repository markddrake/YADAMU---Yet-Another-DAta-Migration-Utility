@set DIR=JSON\MSSQL
@set MDIR=..\JSON\MSSQL 
@set ID=1
@set FILENAME=AdventureWorks
@set SCHEMA=ADVWRK
@set ID=1
mkdir %DIR%
psql -U postgres -h 192.168.1.250 -a -f SQL/JSON_IMPORT.sql
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=%SCHEMA% -v ID=%ID%
call scripts\import_MSSQL_jSax.bat %MDIR% %SCHEMA%%ID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@set ID=2
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=%SCHEMA% -v ID=%ID%
node node\jSaximport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --File=%DIR%\%FILENAME%1.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json