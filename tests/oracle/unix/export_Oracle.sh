export TGT=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_DB_ROOT/node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/HR$VER.json owner=\"HR$SCHEMAVER\" mode=$MODE  logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/SH$VER.json owner=\"SH$SCHEMAVER\" mode=$MODE  logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/OE$VER.json owner=\"OE$SCHEMAVER\" mode=$MODE  logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/PM$VER.json owner=\"PM$SCHEMAVER\" mode=$MODE  logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/IX$VER.json owner=\"IX$SCHEMAVER\" mode=$MODE  logFile=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/BI$VER.json owner=\"BI$SCHEMAVER\" mode=$MODE  logFile=$EXPORTLOG
