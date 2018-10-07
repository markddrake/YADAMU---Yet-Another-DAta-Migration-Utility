mkdir JSON\MSSQL
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f <SQL/JSON_IMPORT.sql
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @SCHEMA='ADVWRK'; SET @ID=1" <TESTS\RECREATE_SCHEMA.sql
call scripts\import_master_MSSQL.bat ADVWRK1
node node\export --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --OWNER=\"ADVWRK1\" --FILE=JSON\MSSQL\AdventureWorks1.json
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @SCHEMA='ADVWRK'; SET @ID=2" <TESTS\RECREATE_SCHEMA.sql
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"ADVWRK2\" --FILE=JSON\MSSQL\AdventureWorks1.json
node node\export --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --OWNER=\"ADVWRK2\" --FILE=JSON\MSSQL\AdventureWorks2.json
dir JSON\MSSQL\*1*.json
dir JSON\MSSQL\*2*.json