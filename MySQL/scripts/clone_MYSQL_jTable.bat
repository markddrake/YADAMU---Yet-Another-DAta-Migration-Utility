@set DIR=JSON\\MYSQL
@set ID=1
@set FILENAME=sakila
@set SCHEMA=sakila
@set ID=1
mkdir %DIR%
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <SQL/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @SCHEMA='%SCHEMA%'; SET @ID=%ID%" <TESTS\RECREATE_SCHEMA.sql
node node\jTableImport --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=..\\JSON\\MYSQL\\%FILENAME%.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@set ID=2
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @SCHEMA='%SCHEMA%'; SET @ID=%ID%" <TESTS\RECREATE_SCHEMA.sql
node node\jTableImport --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=%DIR%\\%FILENAME%1.json toUser=%SCHEMA%%ID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json
