@set SRC=%~1
@set ISCHEMA=%~2
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\Northwind.json
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\HumanResources.json
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\Person.json
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\Production.json
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\Purchasing.json
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\Sales.json
node node\jSaxImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"%ISCHEMA%\" FILE=%SRC%\AdventureWorksDW.json
