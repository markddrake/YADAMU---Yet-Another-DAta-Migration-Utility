export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/HR$FILEVER.json toUser=\"HR$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/SH$FILEVER.json toUser=\"SH$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/OE$FILEVER.json toUser=\"OE$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/PM$FILEVER.json toUser=\"PM$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/IX$FILEVER.json toUser=\"IX$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/BI$FILEVER.json toUser=\"BI$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG