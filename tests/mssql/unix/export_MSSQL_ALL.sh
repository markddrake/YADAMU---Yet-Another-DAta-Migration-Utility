@SET TGT=$1
@SET FILEVER=$2
@SET SCHEMAVER=$3
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=Northwind$SCHEMAVER         owner=dbo               file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Sales             file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Person            file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Production        file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Purchasing        file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=HumanResources    file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorksDW$SCHEMAVER  owner=dbo               file=$TGT/AdventureWorksALL$FILEVER.json mode=$MODE logFile=$EXPORTLOG
