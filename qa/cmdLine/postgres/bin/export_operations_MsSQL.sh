export TGT=$1
export FILEVER=$2
export SCHEMAVER=$3
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"Northwind$SCHEMAVER\"        file=$TGT/Northwind$FILEVER.json        mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"Sales$SCHEMAVER\"            file=$TGT/Sales$FILEVER.json            mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"Person$SCHEMAVER\"           file=$TGT/Person$FILEVER.json           mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"Production$SCHEMAVER\"       file=$TGT/Production$FILEVER.json       mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"Purchasing$SCHEMAVER\"       file=$TGT/Purchasing$FILEVER.json       mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"HumanResources$SCHEMAVER\"   file=$TGT/HumanResources$FILEVER.json   mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME owner=\"AdventureWorksDW$SCHEMAVER\" file=$TGT/AdventureWorksDW$FILEVER.json mode=$MODE log_file=$YADAMU_EXPORT_LOG
