@set SRC=%~1
@set USERID=%~2
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\Northwind.json        TOUSER=\"%USERID%\" 
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\HumanResources.json   TOUSER=\"%USERID%\" 
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\Person.json           TOUSER=\"%USERID%\" 
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\Production.json       TOUSER=\"%USERID%\" 
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\Purchasing.json       TOUSER=\"%USERID%\" 
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\Sales.json            TOUSER=\"%USERID%\" 
node node\jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\AdventureWorksDW.json TOUSER=\"%USERID%\" 
