. env/setEnvironment.sh
export MDIR=$TESTDATA/MySQL
mkdir -p $MDIR
export SCHEMA=sakila
export FILENAME=sakila
node ../node/export --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME --file=$MDIR/$FILENAME.json owner="$SCHEMA" mode=$MODE logFile=$EXPORTLOG
