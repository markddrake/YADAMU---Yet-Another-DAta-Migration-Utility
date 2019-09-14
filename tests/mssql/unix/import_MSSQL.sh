export SRC=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=Northwind$SCHEMAVER         file=$SRC/Northwind$VER.json        to_user=dbo            log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Sales$VER.json            to_user=Sales          log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Person$VER.json           to_user=Person         log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Production$VER.json       to_user=Production     log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Purchasing$VER.json       to_user=Purchasing     log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/HumanResources$VER.json   to_user=HumanResources log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorksDW$SCHEMAVER  file=$SRC/AdventureWorksDW$VER.json to_user=dbo            log_file=$IMPORTLOG mode=$MODE