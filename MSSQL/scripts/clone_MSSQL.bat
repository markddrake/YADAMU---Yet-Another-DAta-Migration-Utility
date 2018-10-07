mkdir JSON\MSSQL
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=ADVWRK -v ID=1 
call scripts\import_master_MSSQL.bat ADVWRK1
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle File=JSON\MSSQL\AdventureWorks1.json owner=ADVWRK1
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMA.sql -v SCHEMA=ADVWRK -v ID=2
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle File=JSON\MSSQL\AdventureWorks1.json touser=ADVWRK2
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --DATABASE=clone --PASSWORD=oracle File=JSON\MSSQL\AdventureWorks2.json owner=ADVWRK2
dir JSON\MSSQL\*1*.json
dir JSON\MSSQL\*2*.json