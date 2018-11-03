@set TNS=%~1
@set DIR=JSON\%TNS%
@set MODE=DATA_ONLY
@set MDIR=..\..\JSON\%TNS%\%MODE%
@set ID=1
mkdir %DIR%
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <..\sql\JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=1" <sql\RECREATE_ORACLE_ALL.sql
call windows\import_oracle_jSax.bat %MDIR% %ID% ""
call windows\export_oracle.bat %DIR% %ID% %ID%
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=2" <sql\RECREATE_ORACLE_ALL.sql
@set ID=2
call windows\import_oracle_jSax.bat %DIR% %ID% 1
call windows\export_oracle.bat %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json