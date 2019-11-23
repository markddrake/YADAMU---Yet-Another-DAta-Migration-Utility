export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=HR$SCHEMAVER file=$SRC/HR$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=SH$SCHEMAVER file=$SRC/SH$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=OE$SCHEMAVER file=$SRC/OE$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=PM$SCHEMAVER file=$SRC/PM$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=IX$SCHEMAVER file=$SRC/IX$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=BI$SCHEMAVER file=$SRC/BI$FILEVER.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG

