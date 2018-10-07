mkdir JSON\MYSQL
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=sakila -v ID=1 
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone File=..\JSON\MYSQL\sakila.json toUser=sakila1
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone File=JSON\MYSQL\sakila1.json owner=sakila1
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=sakila -v ID=2 
node node\import node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle  File=JSON\MYSQL\sakila1.json toUser=sakila2
node node\export node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle  File=JSON\MYSQL\sakila2.json owner=sakila2
dir JSON\MYSQL\*1.json
dir JSON\MYSQL\*2.json
