node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\Northwind.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\HumanResources.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\Person.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\Production.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\Purchasing.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\Sales.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=sys TOUSER=\"%~1\" FILE=..\\JSON\\MSSQL\\AdventureWorksDW.json
