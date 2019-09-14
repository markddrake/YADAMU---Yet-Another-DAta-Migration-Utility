export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/HR$FILEVER.json to_user=\"HR$SCHEMAVER\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/SH$FILEVER.json to_user=\"SH$SCHEMAVER\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/OE$FILEVER.json to_user=\"OE$SCHEMAVER\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/PM$FILEVER.json to_user=\"PM$SCHEMAVER\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/IX$FILEVER.json to_user=\"IX$SCHEMAVER\" mode=$MODE log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/BI$FILEVER.json to_user=\"BI$SCHEMAVER\" mode=$MODE log_file=$IMPORTLOG