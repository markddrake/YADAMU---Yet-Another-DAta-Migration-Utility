@set SRC=%~1
@set SCHEMA=%~2
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\Northwind.json
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\HumanResources.json
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\Person.json
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\Production.json
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\Purchasing.json
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\Sales.json
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%SCHEMA%\" FILE=%SRC%\AdventureWorksDW.json
