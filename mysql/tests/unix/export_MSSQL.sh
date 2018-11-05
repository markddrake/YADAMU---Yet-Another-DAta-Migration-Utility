export TGT=$1
export VER=$2
export ID=$3
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"Northwind$USRID\"       --FILE=$TGT/Northwind$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"Sales$USRID\"           --FILE=$TGT/Sales$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"Person$USRID\"          --FILE=$TGT/Person$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"Production$USRID\"      --FILE=$TGT/Production$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"Purchasing$USRID\"      --FILE=$TGT/Purchasing$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"HumanResources$USRID\"  --FILE=$TGT/HumanResources$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME --OWNER=\"DW$USRID\"              --FILE=$TGT/AdventureWorksDW$VER.json
