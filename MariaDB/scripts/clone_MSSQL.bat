@set DIR=JSON\MSSQL
@set MDIR=..\JSON\MSSQL 
@set ID=1
@set FILENAME=AdventureWorks
@set SCHEMA=ADVWRK
@set ID=1
mkdir %DIR%
mysql -uroot -poracle -h192.168.1.250 -Dmysql -P3307 -v -f <SQL/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dmysql -P3307 -v -f --init-command="SET @SCHEMA='%SCHEMA%'; SET @ID=%ID%" <TESTS\RECREATE_SCHEMA.sql
call scripts\import_MSSQL.bat %MDIR% %SCHEMA%%ID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@set ID=2
mysql -uroot -poracle -h192.168.1.250 -Dmysql -P3307 -v -f --init-command="SET @SCHEMA='%SCHEMA%'; SET @ID=2" <TESTS\RECREATE_SCHEMA.sql
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=%DIR%\%FILENAME%1.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json