export TGT=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$TGT/HR$FILEVER.json owner=\"HR$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$TGT/SH$FILEVER.json owner=\"SH$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$TGT/OE$FILEVER.json owner=\"OE$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$TGT/PM$FILEVER.json owner=\"PM$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$TGT/IX$FILEVER.json owner=\"IX$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$TGT/BI$FILEVER.json owner=\"BI$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
