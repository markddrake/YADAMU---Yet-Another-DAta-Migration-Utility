node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=Northwind --OWNER=\"dbo\" --FILE=JSON\Northwind.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorksDW --OWNER=\"dbo\" --FILE=JSON\AdventureWorksDW.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks --OWNER=\"HumanResources\" --FILE=c:JSON\\Sales.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks --OWNER=\"Person\" --FILE=c:JSON\\Person.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks --OWNER=\"Production\" --FILE=c:JSON\\Production.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks --OWNER=\"Purchasing\" --FILE=c:JSON\\Purchasing.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks --OWNER=\"Sales\" --FILE=c:JSON\\HumanResources.json
