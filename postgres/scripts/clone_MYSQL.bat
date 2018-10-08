mkdir JSON\MYSQL
psql -U postgres -h 192.168.1.250 -a -f SQL/JSON_IMPORT.sql
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=SAKILA -v ID=1 
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  File=..\JSON\MYSQL\sakila.json toUser=sakila1
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  File=JSON\MYSQL\sakila1.json owner=sakila1
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=SAKILA -v ID=2
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  File=JSON\MYSQL\sakila1.json toUser=sakila2
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  File=JSON\MYSQL\sakila2.json owner=sakila2
dir JSON\MYSQL\*1.json
dir JSON\MYSQL\*2.json
