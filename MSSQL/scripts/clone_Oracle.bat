mkdir JSON\Oracle
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_ORACLE_ALL.sql -v ID=1 
call scripts\import_master_oracle.bat 1 18c DATA_ONLY
call scripts\export_oracle_all.bat 1 1
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_ORACLE_ALL.sql -v ID=2 
call scripts\import_oracle_all.bat 2 1 
call scripts\export_oracle_all.bat 2 2 
dir JSON\Oracle\*1*.json
dir JSON\Oracle\*2*.json
