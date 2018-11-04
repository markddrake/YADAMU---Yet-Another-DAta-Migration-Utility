export SRC=$1
export USRID=$2
export VER=$3
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/Northwind$VER.json        --TOUSER=/"Northwind$USRID/" 
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/Sales$VER.json            --TOUSER=/"Sales$USRID/" 
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/Person$VER.json           --TOUSER=/"Person$USRID/" 
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/Production$VER.json       --TOUSER=/"Production$USRID/" 
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/Purchasing$VER.json       --TOUSER=/"Purchasing$USRID/" 
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/HumanResources$VER.json   --TOUSER=/"HumanResources$USRID/" 
node ../node/import  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --FILE=$SRC/AdventureWorksDW$VER.json --TOUSER=/"DW$USRID/" 
