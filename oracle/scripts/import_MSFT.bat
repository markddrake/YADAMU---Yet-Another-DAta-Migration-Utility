sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMA MSFT 1
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\Northwind.json
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\HumanResources.json
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\Person.json
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\Production.json
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\Purchasing.json
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\Sales.json
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MSFT1\" FILE=..\JSON\MSSQL\AdventureWorksDW.json
