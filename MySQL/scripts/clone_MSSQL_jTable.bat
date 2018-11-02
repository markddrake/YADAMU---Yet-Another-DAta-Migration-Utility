@set DIR=JSON\MSSQL
@set MDIR=..\JSON\MSSQL 
@set ID=1
@set SCHEMA=ADVWRK
@set FILENAME=AdventureWorks
mkdir %DIR%
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <SQL/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=%ID%" <TESTS\RECREATE_MSSQL_ALL.sql
call scripts\import_MSSQL_jTable.bat %MDIR% %ID% ""
call scripts\export_MSSQL %DIR% %ID% %ID%
@set ID=2
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=%ID%" <TESTS\RECREATE_MSSQL_ALL.sql
call scripts\import_MSSQL_jTable.bat %DIR% %ID% 1
call scripts\export_MSSQL %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json