source qa/bin/initialize.sh $BASH_SOURCE[0] $BASH_SOURCE[0] mysql export
export YADAMU_OUTPUT_BASE=$YADAMU_HOME\JSON\
if [ ! -e $YADAMU_OUTPUT_BASE ]; then mkidr -p $YADAMU_OUTPUT_BASE; fi
export YADAMU_OUTPUT_PATH=$YADAMU_OUTPUT_BASE%\$YADAMU_TARGET
if [ ! -e $YADAMU_OUTPUT_PATH ]; then mkidr -p $YADAMU_OUTPUT_PATH; fi
export MODE=DATA_ONLY
export FILENAME=sakila
export SCHEMA=sakila
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME --file=$YADAMU_INPUT_PATH/$FILENAME.json owner=\"$SCHEMA\" mode=$MODE log_file=$YADAMU_EXPORT_LOG
export FILENAME=jsonExample
export SCHEMA=jtest
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME --file=$YADAMU_INPUT_PATH/$FILENAME.json owner=\"$SCHEMA\" mode=$MODE log_file=$YADAMU_EXPORT_LOG
