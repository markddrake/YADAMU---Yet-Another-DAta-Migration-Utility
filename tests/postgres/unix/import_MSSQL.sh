export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/Northwind$FILEVER.json        toUser=\"Northwind$SCHEMAVER\"      logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/Sales$FILEVER.json            toUser=\"Sales$SCHEMAVER\"          logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/Person$FILEVER.json           toUser=\"Person$SCHEMAVER\"         logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/Production$FILEVER.json       toUser=\"Production$SCHEMAVER\"     logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/Purchasing$FILEVER.json       toUser=\"Purchasing$SCHEMAVER\"     logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/HumanResources$FILEVER.json   toUser=\"HumanResources$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/AdventureWorksDW$FILEVER.json toUser=\"DW$SCHEMAVER\"             logFile=$IMPORTLOG mode=$MODE
