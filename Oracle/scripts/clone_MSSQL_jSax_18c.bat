@set TNS=ORCL18c
@set MDIR=..\JSON\MSSQL
@set DIR=JSON\%TNS%\MSSQL
@set UID=ADVWRK
mkdir %DIR%
sqlplus system/oracle@%TNS% @SQL/COMPILE_ALL
sqlplus system/oracle@%TNS% @TESTS/RECREATE_SCHEMA.sql %UID% 1
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\Northwind.json
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\HumanResources.json
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\Person.json
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\Production.json
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\Purchasing.json
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\Sales.json
node node\jSaxImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%UID%1\" FILE=%MDIR%\AdventureWorksDW.json
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\AdventureWorks1.json owner=%UID%1
sqlplus system/oracle@%TNS% @TESTS/RECREATE_SCHEMA.sql %UID% 2
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\AdventureWorks1.json touser=%UID%2
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\AdventureWorks2.json owner=%UID%2
dir %DIR%\*1*.json
dir %DIR%\*2*.json
