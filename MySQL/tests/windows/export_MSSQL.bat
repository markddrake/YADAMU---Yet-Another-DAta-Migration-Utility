@SET TGT=%~1
@SET VER=%~2
@SET ID=%~3
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"Northwind%UID%\"       --FILE=%TGT%\Northwind%VER%.json
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"Sales%UID%\"           --FILE=%TGT%\Sales%VER%.json
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"Person%UID%\"          --FILE=%TGT%\Person%VER%.json
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"Production%UID%\"      --FILE=%TGT%\Production%VER%.json
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"Purchasing%UID%\"      --FILE=%TGT%\Purchasing%VER%.json
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"HumanResources%UID%\"  --FILE=%TGT%\HumanResources%VER%.json
node ..\node\export  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --OWNER=\"DW%UID%\"              --FILE=%TGT%\AdventureWorksDW%VER%.json
