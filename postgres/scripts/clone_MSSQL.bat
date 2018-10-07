mkdir JSON\MSSQL
psql -U postgres -h 192.168.1.250 -a -f SQL/JSON_IMPORT.sql
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=ADVWRK -v ID=1
call scripts\import_master_MSSQL.bat ADVWRK1
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\MSSQL\AdventureWorks1.json owner=ADVWRK1
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMA.sql -a -v SCHEMA=ADVWRK -v ID=2 
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\MSSQL\AdventureWorks1.json touser=ADVWRK2
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\MSSQL\AdventureWorks2.json owner=ADVWRK2
dir JSON\MSSQL\*1*.json
dir JSON\MSSQL\*2*.json