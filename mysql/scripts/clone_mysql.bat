node node\export --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --OWNER=\"sakila\" --FILE=JSON\sakila.json
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f <SQL/JSON_IMPORT.sql
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @SCHEMA='sakila'; SET @ID=1" <TESTS\RECREATE_SCHEMA.sql
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"salika1\" --FILE=JSON\\sakila.json
node node\export --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --OWNER=\"salika1\" --FILE=JSON\salika1.json
