mkdir JSON\Oracle\18c
psql -U postgres -h 192.168.1.250 -a -f SQL/JSON_IMPORT.sql
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_ORACLE_ALL.sql -a -v ID=1
call scripts\import_master_oracle.bat 1 18c DATA_ONLY
call scripts\export_oracle_all.bat 1 1 DATA_ONLY
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_ORACLE_ALL.sql -a -v ID=2
call scripts\import_oracle_all.bat 2 1 
call scripts\export_oracle_all.bat 2 2 DATA_ONLY
dir JSON\Oracle\18c\*1*.json
dir JSON\Oracle\18c\*2*.json
