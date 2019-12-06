export TGT=$1
export FILEVER=$2
export SCHEMAVER=$3
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=Northwind$SCHEMAVER         owner=dbo               file=$TGT/Northwind$FILEVER.json        mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Sales             file=$TGT/Sales$FILEVER.json            mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Person            file=$TGT/Person$FILEVER.json           mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Production        file=$TGT/Production$FILEVER.json       mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=Purchasing        file=$TGT/Purchasing$FILEVER.json       mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorks$SCHEMAVER    owner=HumanResources    file=$TGT/HumanResources$FILEVER.json   mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --HOSTNAME=$DB_HOST --password=$DB_PWD --database=AdventureWorksDW$SCHEMAVER  owner=dbo               file=$TGT/AdventureWorksDW$FILEVER.json mode=$MODE log_file=$YADAMU_EXPORT_LOG
