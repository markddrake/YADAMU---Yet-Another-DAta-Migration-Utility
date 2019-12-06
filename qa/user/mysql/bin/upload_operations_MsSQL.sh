export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Northwind$FILEVER.json        to_user=\"Northwind$SCHEMAVER\"        mode=$MODE log_file=$YADAMU_IMPORT_LOG  
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Sales$FILEVER.json            to_user=\"Sales$SCHEMAVER\"            mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Person$FILEVER.json           to_user=\"Person$SCHEMAVER\"           mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Production$FILEVER.json       to_user=\"Production$SCHEMAVER\"       mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Purchasing$FILEVER.json       to_user=\"Purchasing$SCHEMAVER\"       mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/HumanResources$FILEVER.json   to_user=\"HumanResources$SCHEMAVER\"   mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/AdventureWorksDW$FILEVER.json to_user=\"AdventureWorksDW$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
