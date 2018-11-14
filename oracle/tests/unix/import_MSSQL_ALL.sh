export SRC=$1
export USCHEMA=$2
export SCHVER=$3
export VER=$4
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/Northwind$VER.json        toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/Sales$VER.json            toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/Person$VER.json           toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/Production$VER.json       toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/Purchasing$VER.json       toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/HumanResources$VER.json   toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
node ../node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION FILE=$SRC/AdventureWorksDW$VER.json toUser=\"$USCHEMA$SCHVER\" logfile=$IMPORTLOG
