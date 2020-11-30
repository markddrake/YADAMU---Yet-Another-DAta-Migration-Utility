export SRC=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/HR$VER.json to_user=\"HR$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/SH$VER.json to_user=\"SH$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/OE$VER.json to_user=\"OE$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/PM$VER.json to_user=\"PM$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/IX$VER.json to_user=\"IX$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$SRC/BI$VER.json to_user=\"BI$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG

