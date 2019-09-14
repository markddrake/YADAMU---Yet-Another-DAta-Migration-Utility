export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/Northwind$FILEVER.json        to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/Sales$FILEVER.json            to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/Person$FILEVER.json           to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/Production$FILEVER.json       to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/Purchasing$FILEVER.json       to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/HumanResources$FILEVER.json   to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$SRC/AdventureWorksDW$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$IMPORTLOG