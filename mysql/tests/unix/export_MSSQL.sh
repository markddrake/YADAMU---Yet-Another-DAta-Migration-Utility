export TGT=$1
export VER=$2
export ID=$3
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"Northwind$USRID/"       --FILE=$TGT/Northwind$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"Sales$USRID/"           --FILE=$TGT/Sales$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"Person$USRID/"          --FILE=$TGT/Person$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"Production$USRID/"      --FILE=$TGT/Production$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"Purchasing$USRID/"      --FILE=$TGT/Purchasing$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"HumanResources$USRID/"  --FILE=$TGT/HumanResources$VER.json
node ../node/export  --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBASE --OWNER=/"DW$USRID/"              --FILE=$TGT/AdventureWorksDW$VER.json
