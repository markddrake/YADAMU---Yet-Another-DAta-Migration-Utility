@set MDIR=..\JSON\MYSQL
@SET DIR=JSON\MYSQL
@SET SCHEMA=SAKILA
@SET FILENAME=sakila
@SET ID=1
mkdir %DIR%
psql -U postgres -h 192.168.1.250 -a -f SQL/JSON_IMPORT.sql
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=%SCHEMA% -v ID=%ID%
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=%MDIR%\%FILENAME%.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@SET ID=2
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=%SCHEMA% -v ID=%ID%
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=%DIR%\%FILENAME%1.json touser=%SCHEMA%%ID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json
