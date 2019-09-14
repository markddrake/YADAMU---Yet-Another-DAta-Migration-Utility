export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=Northwind$SCHEMAVER         file=$SRC/Northwind$FILEVER.json        to_user=dbo            log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Sales$FILEVER.json            to_user=Sales          log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Person$FILEVER.json           to_user=Person         log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Production$FILEVER.json       to_user=Production     log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/Purchasing$FILEVER.json       to_user=Purchasing     log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    file=$SRC/HumanResources$FILEVER.json   to_user=HumanResources log_file=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=AdventureWorksDW$SCHEMAVER  file=$SRC/AdventureWorksDW$FILEVER.json to_user=dbo            log_file=$IMPORTLOG mode=$MODE