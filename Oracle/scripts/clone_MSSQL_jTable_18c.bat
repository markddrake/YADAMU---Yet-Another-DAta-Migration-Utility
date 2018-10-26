@set TNS=ORCL18c
@set MDIR=..\JSON\MSSQL
@set DIR=JSON\%TNS%\MSSQL
@set SCHEMA=ADVWRK
mkdir %DIR%
sqlplus system/oracle@%TNS% @SQL/COMPILE_ALL
sqlplus system/oracle@%TNS% @TESTS/RECREATE_SCHEMA.sql %SCHEMA% 1
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\Northwind.json
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\HumanResources.json
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\Person.json
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\Production.json
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\Purchasing.json
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\Sales.json
node node\jTableImport userid=SYSTEM/oracle@%TNS% TOUSER=\"%SCHEMA%1\" FILE=%MDIR%\AdventureWorksDW.json
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\AdventureWorks1.json owner=%SCHEMA%1
sqlplus system/oracle@%TNS% @TESTS/RECREATE_SCHEMA.sql %SCHEMA% 2
node node\jTableImport userid=SYSTEM/oracle@%TNS% File=%DIR%\AdventureWorks1.json touser=%SCHEMA%2
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\AdventureWorks2.json owner=%SCHEMA%2
dir %DIR%\*1*.json
dir %DIR%\*2*.json
