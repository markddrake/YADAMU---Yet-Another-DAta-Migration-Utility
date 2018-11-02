@set SRC=%~1
@set UID=%~2
@set VER=%~3
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\Northwind%VER%.json        --TOUSER=\"Northwind%UID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\Sales%VER%.json            --TOUSER=\"Sales%UID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\Person%VER%.json           --TOUSER=\"Person%UID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\Production%VER%.json       --TOUSER=\"Production%UID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\Purchasing%VER%.json       --TOUSER=\"Purchasing%UID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\HumanResources%VER%.json   --TOUSER=\"HumanResources%UID%\" 
node node\jTableImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=%SRC%\AdventureWorksDW%VER%.json --TOUSER=\"DW%UID%\" 
