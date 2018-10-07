mkdir JSON\MYSQL
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f <SQL/JSON_IMPORT.sql
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @SCHEMA='sakila'; SET @ID=1" <TESTS\RECREATE_SCHEMA.sql
node node\import node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys --File=..\\JSON\\MYSQL\\sakila.json toUser=sakila1
node node\export node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys --File=JSON\\MYSQL\\sakila1.json owner=sakila1
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @SCHEMA='sakila'; SET @ID=2" <TESTS\RECREATE_SCHEMA.sql
node node\import node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys --File=JSON\\MYSQL\\sakila1.json toUser=sakila2
node node\export node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys --File=JSON\MYSQL\sakila2.json owner=sakila2
dir JSON\MYSQL\*1.json
dir JSON\MYSQL\*2.json
