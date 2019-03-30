export YADAMU_TARGET=MsSQL/jTable
export YADAMU_PARSER=RDBMS
. ../unix/initialize.sh $(readlink -f "$BASH_SOURCE")
export YADAMU_INPUT_PATH=${YADAMU_INPUT_PATH:0:-7}
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_DB_ROOT/sql/JSON_IMPORT.sql >$YADAMU_LOG_PATH/install/JSON_IMPORT.log
export SCHEMAVER=1
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @ID=$SCHEMAVER; then export @METHOD='$YADAMU_PARSER';" <$YADAMU_SCRIPT_ROOT/sql/RECREATE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
. $YADAMU_SCRIPT_ROOT/unix/jTableImport_MSSQL.sh $YADAMU_INPUT_PATH $SCHEMAVER ""
. $YADAMU_SCRIPT_ROOT/unix/export_MSSQL.sh $YADAMU_OUTPUT_PATH $SCHEMAVER $SCHEMAVER
export SCHEMAVER=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @ID=$SCHEMAVER; then export @METHOD='$YADAMU_PARSER';" <$YADAMU_SCRIPT_ROOT/sql/RECREATE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
. $YADAMU_SCRIPT_ROOT/unix/jTableImport_MSSQL.sh $YADAMU_OUTPUT_PATH $SCHEMAVER 1
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT --init-command="export @ID1=1; then export @ID2=$SCHEMAVER; then export @METHOD='$YADAMU_PARSER'" --table <$YADAMU_SCRIPT_ROOT/sql/COMPARE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
. $YADAMU_SCRIPT_ROOT/unix/export_MSSQL.sh $YADAMU_OUTPUT_PATH $SCHEMAVER $SCHEMAVER
export FILENAME=AdventureWorksALL
export SCHEMA=ADVWRK
export SCHEMAVER=1
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT  -v -f --init-command="export @SCHEMA='$SCHEMA$SCHEMAVER'; then export @METHOD='$YADAMU_PARSER'" <$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_INPUT_PATH/$FILENAME.json toUser=\"$SCHEMA$SCHEMAVER\" mode=$MODE  logFile=$IMPORTLOG
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT --init-command="export @SCHEMA='$SCHEMA'; then export @ID1=''; then export @ID2=$SCHEMAVER; then export @METHOD='$YADAMU_PARSER'" --table  <$YADAMU_SCRIPT_ROOT/sql/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE logFile=$EXPORTLOG
export SCHEMAVER=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT  -v -f --init-command="export @SCHEMA='$SCHEMA$SCHEMAVER'; then export @METHOD='$YADAMU_PARSER'" <$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/${FILENAME}1.json toUser=\"$SCHEMA$SCHEMAVER\" mode=$MODE logFile=$IMPORTLOG
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT --init-command="export @SCHEMA='$SCHEMA'; then export @ID1=1; then export @ID2=$SCHEMAVER; then export @METHOD='$YADAMU_PARSER'" --table  <$YADAMU_SCRIPT_ROOT/sql/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --port=$DB_PORT --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE logFile=$EXPORTLOG
node $YADAMU_HOME/utilities/node/compareFileSizes $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH
node --max_old_space_size=4096 $YADAMU_HOME/utilities/node/compareArrayContent $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH false