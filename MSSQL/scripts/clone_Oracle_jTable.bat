@set DIR=JSON\Oracle
@set MODE=DATA_ONLY
@set MDIR=../JSON/ORCL18c/DATA_ONLY
@set ID=1
mkdir %DIR%
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <SQL/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=1" <TESTS\RECREATE_ORACLE_ALL.sql
call scripts\import_oracle_jTable.bat %MDIR% %ID% ""
call scripts\export_oracle.bat %DIR% %ID% %ID%
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=2" <TESTS\RECREATE_ORACLE_ALL.sql
@set ID=2
call scripts\import_oracle_jTable.bat %DIR% %ID% 1
call scripts\export_oracle.bat %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json