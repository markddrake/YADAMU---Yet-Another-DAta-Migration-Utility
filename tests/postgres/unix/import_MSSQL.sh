export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/Northwind$FILEVER.json        to_user=\"Northwind$SCHEMAVER\"      log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/Sales$FILEVER.json            to_user=\"Sales$SCHEMAVER\"          log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/Person$FILEVER.json           to_user=\"Person$SCHEMAVER\"         log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/Production$FILEVER.json       to_user=\"Production$SCHEMAVER\"     log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/Purchasing$FILEVER.json       to_user=\"Purchasing$SCHEMAVER\"     log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/HumanResources$FILEVER.json   to_user=\"HumanResources$SCHEMAVER\" log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/AdventureWorksDW$FILEVER.json to_user=\"AdventureWorksDW$SCHEMAVER\"             log_file=$IMPORTLOG mode=$MODE
