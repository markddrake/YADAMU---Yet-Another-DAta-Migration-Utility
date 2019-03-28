export SRC=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_DB_ROOT/node/import userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/HR$VER.json toUser=\"HR$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/SH$VER.json toUser=\"SH$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/OE$VER.json toUser=\"OE$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/PM$VER.json toUser=\"PM$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/IX$VER.json toUser=\"IX$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/import userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/BI$VER.json toUser=\"BI$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG

