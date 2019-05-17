export YADAMU_TARGET=MySQL
export YADAMU_PARSER=CLARINET
. ../unix/initialize.sh $(readlink -f "$BASH_SOURCE")
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i$YADAMU_DB_ROOT/sql/YADAMU_IMPORT.sql > $YADAMU_LOG_PATH/install/YADAMU_IMPORT.log
export FILENAME=sakila
export SCHEMA=sakila
export SCHEMAVER=1
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_INPUT_PATH/$FILENAME.json toUser=\"dbo\" mode=$MODE  logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE logFile=$EXPORTLOG
export SCHEMAVER=2
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME1.json toUser=\"dbo\" mode=$MODE logFile=$IMPORTLOG
export ID1=1
export ID2=$SCHEMAVER
export METHOD=$YADAMU_PARSER
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE logFile=$EXPORTLOG
export FILENAME=jsonExample
export SCHEMA=jtest
export SCHEMAVER=1
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_INPUT_PATH/$FILENAME.json toUser=\"dbo\" mode=$MODE  logFile=$IMPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE logFile=$EXPORTLOG
export SCHEMAVER=2
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME1.json toUser=\"dbo\" mode=$MODE logFile=$IMPORTLOG
export ID1=1
export ID2=$SCHEMAVER
export METHOD=$YADAMU_PARSER
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE logFile=$EXPORTLOG
node $YADAMU_HOME/utilities/node/compareFileSizes $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH
node $YADAMU_HOME/utilities/node/compareArrayContent $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH false