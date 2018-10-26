@set DIR=JSON\Oracle\ORCL18C
@set MODE=DATA_ONLY
@set MDIR=../JSON/ORCL18c/DATA_ONLY
@set ID=1
mkdir %DIR%
psql -U postgres -h 192.168.1.250 -a -f SQL/JSON_IMPORT.sql
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_ORACLE_ALL.sql -a -v ID=%ID%
call scripts\import_oracle_jTable.bat %MDIR% %ID% ""
call scripts\export_oracle.bat %DIR% %ID% %ID%
@set ID=2
psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_ORACLE_ALL.sql -a -v ID=%ID%
call scripts\import_oracle_jTable.bat %DIR% %ID% 1
call scripts\export_oracle.bat %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json