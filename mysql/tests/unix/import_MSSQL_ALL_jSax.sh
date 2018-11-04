export SRC=$~1
export USCHEMA=$~2
export UID=$~3
export VER=$~4
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/Northwind$VER.json        --TOUSER=/"$USCHEMA$UID/" 
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/Sales$VER.json            --TOUSER=/"$USCHEMA$UID/" 
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/Person$VER.json           --TOUSER=/"$USCHEMA$UID/" 
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/Production$VER.json       --TOUSER=/"$USCHEMA$UID/" 
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/Purchasing$VER.json       --TOUSER=/"$USCHEMA$UID/" 
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/HumanResources$VER.json   --TOUSER=/"$USCHEMA$UID/" 
node ../node/jSaxImport  --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3306 --DATABASE=sys --FILE=$SRC/AdventureWorksDW$VER.json --TOUSER=/"$USCHEMA$UID/" 
