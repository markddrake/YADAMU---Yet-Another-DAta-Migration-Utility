mkdir JSON\MYSQL
mysql -uroot -poracle -h192.168.1.250 -Dmysql -P3307 -v -f <SQL/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dmysql -P3307 -v -f --init-command="SET @SCHEMA='sakila'; SET @ID=1" <TESTS\RECREATE_SCHEMA.sql
node client\import --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=..\JSON\MYSQL\sakila.json toUser=sakila1
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=JSON\MYSQL\sakila1.json owner=sakila1
mysql -uroot -poracle -h192.168.1.250 -Dmysql -P3307 -v -f --init-command="SET @SCHEMA='sakila'; SET @ID=2" <TESTS\RECREATE_SCHEMA.sql
node client\import --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=JSON\MYSQL\sakila1.json toUser=sakila2
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3307 --PASSWORD=oracle --DATABASE=mysql --File=JSON\MYSQL\sakila2.json owner=sakila2
dir JSON\MYSQL\*1.json
dir JSON\MYSQL\*2.json
