if [ -z ${MODE+x} ]; then export MODE=DATA_ONLY; fi
export YADAMU_TARGET=MySQL
. ../unix/initialize.sh $(readlink -f "$BASH_SOURCE")
if [ ! -e $YADAMU_INPUT_PATH ]; then mkdir $YADAMU_INPUT_PATH;fi
export FILENAME=jsonExample
export SCHEMA=jtest
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME --file=$YADAMU_INPUT_PATH/$FILENAME.json owner=\"$SCHEMA\" mode=$MODE logFile=$EXPORTLOG
export FILENAME=sakila
export SCHEMA=sakila
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME --file=$YADAMU_INPUT_PATH/$FILENAME.json owner=\"$SCHEMA\" mode=$MODE logFile=$EXPORTLOG