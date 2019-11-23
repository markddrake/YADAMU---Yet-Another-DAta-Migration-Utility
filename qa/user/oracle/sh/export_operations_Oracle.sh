export TGT=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/HR$VER.json owner=\"HR$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/SH$VER.json owner=\"SH$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/OE$VER.json owner=\"OE$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/PM$VER.json owner=\"PM$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/IX$VER.json owner=\"IX$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/BI$VER.json owner=\"BI$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
