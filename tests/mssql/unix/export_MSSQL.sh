@SET TGT=$1
@SET FILEVER=$2
@SET SCHEMAVER=$3
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=Northwind$SCHEMAVER         owner=dbo               file=$TGT/Northwind$FILEVER.json        mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Sales             file=$TGT/Sales$FILEVER.json            mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Person            file=$TGT/Person$FILEVER.json           mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Production        file=$TGT/Production$FILEVER.json       mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Purchasing        file=$TGT/Purchasing$FILEVER.json       mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=HumanResources    file=$TGT/HumanResources$FILEVER.json   mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorksDW$SCHEMAVER  owner=dbo               file=$TGT/AdventureWorksDW$FILEVER.json mode=$MODE logFile=$EXPORTLOG
