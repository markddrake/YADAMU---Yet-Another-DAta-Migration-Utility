export SRC=$1
export SCHEMAVER=$2
export FILEVER=$3
node $YADAMU_DB_ROOT/node/jTableImport  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/HR$FILEVER.json to_user=\"HR$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/SH$FILEVER.json to_user=\"SH$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/OE$FILEVER.json to_user=\"OE$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/PM$FILEVER.json to_user=\"PM$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/IX$FILEVER.json to_user=\"IX$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/jTableImport  --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/BI$FILEVER.json to_user=\"BI$SCHEMAVER\" log_file=$IMPORTLOG