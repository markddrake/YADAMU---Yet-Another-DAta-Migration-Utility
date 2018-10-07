mkdir JSON\Oracle
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f <SQL/JSON_IMPORT.sql
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @ID=1" <TESTS\RECREATE_ORACLE_ALL.sql
call scripts\import_master_oracle.bat 1 18c DATA_ONLY
call scripts\export_oracle_all.bat 1 1
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @ID=2" <TESTS\RECREATE_ORACLE_ALL.sql
call scripts\import_oracle_all.bat 2 1 
call scripts\export_oracle_all.bat 2 2
dir JSON\Oracle\*1*.json
dir JSON\Oracle\*2*.json
