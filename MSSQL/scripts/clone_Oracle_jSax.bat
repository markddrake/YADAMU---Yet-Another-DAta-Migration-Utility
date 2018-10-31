@set DIR=JSON\Oracle
@set MODE=DATA_ONLY
@set MDIR=../JSON/ORCL18c/DATA_ONLY
@set ID=1
mkdir %DIR%
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_ORACLE_ALL.sql -v ID=%ID%
call scripts\import_oracle_jSax.bat %MDIR% %ID% ""
call scripts\export_oracle.bat %DIR% %ID% %ID%
@set ID=2
sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_ORACLE_ALL.sql -v ID=%ID%
call scripts\import_oracle_jSax.bat %DIR% %ID% 1
call scripts\export_oracle.bat %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json