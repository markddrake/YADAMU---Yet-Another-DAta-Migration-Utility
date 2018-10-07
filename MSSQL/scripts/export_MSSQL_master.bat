node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=Northwind        --OWNER=\"dbo\"            --FILE=JSON\Northwind.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorksDW --OWNER=\"dbo\"            --FILE=JSON\AdventureWorksDW.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks   --OWNER=\"HumanResources\" --FILE=JSON\Sales.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks   --OWNER=\"Person\"         --FILE=JSON\Person.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks   --OWNER=\"Production\"     --FILE=JSON\Production.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks   --OWNER=\"Purchasing\"     --FILE=JSON\Purchasing.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks   --OWNER=\"Sales\"          --FILE=JSON\HumanResources.json
