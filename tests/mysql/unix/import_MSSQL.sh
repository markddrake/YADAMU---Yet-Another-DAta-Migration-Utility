export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Northwind$FILEVER.json        toUser=\"Northwind$SCHEMAVER\"      logFile=$IMPORTLOG  
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Sales$FILEVER.json            toUser=\"Sales$SCHEMAVER\"          logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Person$FILEVER.json           toUser=\"Person$SCHEMAVER\"         logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Production$FILEVER.json       toUser=\"Production$SCHEMAVER\"     logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Purchasing$FILEVER.json       toUser=\"Purchasing$SCHEMAVER\"     logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/HumanResources$FILEVER.json   toUser=\"HumanResources$SCHEMAVER\" logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/AdventureWorksDW$FILEVER.json toUser=\"AdventureWorksDW$SCHEMAVER\"             logFile=$IMPORTLOG
