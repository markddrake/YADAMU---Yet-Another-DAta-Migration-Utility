export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/HR$FILEVER.json to_user=\"HR$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/SH$FILEVER.json to_user=\"SH$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/OE$FILEVER.json to_user=\"OE$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/PM$FILEVER.json to_user=\"PM$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/IX$FILEVER.json to_user=\"IX$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/BI$FILEVER.json to_user=\"BI$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG

