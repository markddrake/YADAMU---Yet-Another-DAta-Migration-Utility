@set DIR=JSON\MSSQL
@set MDIR=..\JSON\MSSQL 
@set ID=1
mkdir %DIR%
sqlcmd -Usa -Poracle -S192.168.1.250 -d master -I -e -i TESTS\RECREATE_MSSQL_ALL.sql -v ID=%ID%
sqlcmd -Usa -Poracle -S192.168.1.250 -d Northwind%ID% -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d AdventureWorks%ID% -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d AdventureWorksDW%ID% -I -e -i SQL\JSON_IMPORT.sql
call scripts\import_MSSQL_jSax.bat %MDIR% ""  %ID%
call scripts\export_MSSQL.bat %DIR% %ID% %ID%
@set ID=2
sqlcmd -Usa -Poracle -S192.168.1.250 -d master -I -e -i TESTS\RECREATE_MSSQL_ALL.sql -v ID=%ID%
sqlcmd -Usa -Poracle -S192.168.1.250 -d Northwind%ID% -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d AdventureWorks%ID% -I -e -i SQL\JSON_IMPORT.sql
sqlcmd -Usa -Poracle -S192.168.1.250 -d AdventureWorksDW%ID% -I -e -i SQL\JSON_IMPORT.sql
call scripts\import_MSSQL_jSax.bat %DIR% 1 %ID%
call scripts\export_MSSQL.bat %DIR% %ID% %ID%
dir %DIR%\*1.json
dir %DIR%\*2.json