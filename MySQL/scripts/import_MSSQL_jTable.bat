@set SRC=%~1
@set USERID=%~2
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\Northwind.json        TOUSER=\"%USERID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\HumanResources.json   TOUSER=\"%USERID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\Person.json           TOUSER=\"%USERID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\Production.json       TOUSER=\"%USERID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\Purchasing.json       TOUSER=\"%USERID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\Sales.json            TOUSER=\"%USERID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --File=%SRC%\AdventureWorksDW.json TOUSER=\"%USERID%\" 
