export SRC=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/Northwind$VER.json        toUser=\"Northwind$SCHEMAVER\"      mode=$MODE logFile=$IMPORTLOG 
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/Sales$VER.json            toUser=\"Sales$SCHEMAVER\"          mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/Person$VER.json           toUser=\"Person$SCHEMAVER\"         mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/Production$VER.json       toUser=\"Production$SCHEMAVER\"     mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/Purchasing$VER.json       toUser=\"Purchasing$SCHEMAVER\"     mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/HumanResources$VER.json   toUser=\"HumanResources$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import  userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/AdventureWorksDW$VER.json toUser=\"DW$SCHEMAVER\"             mode=$MODE logFile=$IMPORTLOG
