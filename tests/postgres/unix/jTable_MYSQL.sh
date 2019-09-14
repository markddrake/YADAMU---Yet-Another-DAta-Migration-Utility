export YADAMU_TARGET=MySQL/jTable
export YADAMU_PARSER=RDBMS
. ../unix/initialize.sh $(readlink -f "$BASH_SOURCE")
export YADAMU_INPUT_PATH=${YADAMU_INPUT_PATH:0:-7}
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f $YADAMU_DB_ROOT/sql/YADAMU_IMPORT.sql >> $YADAMU_LOG_PATH/install/YADAMU_IMPORT.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f $YADAMU_SCRIPT_ROOT/sql/YADAMU_TEST.sql >> $YADAMU_LOG_PATH/install/YADAMU_TEST.log
export FILENAME=sakila
export SCHEMA=sakila
export SCHEMAVER=1
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vSCHEMA=$SCHEMA -vID=$SCHEMAVER -vMETHOD=$YADAMU_PARSER/ -f $YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_INPUT_PATH/$FILENAME.json to_user=\"$SCHEMA$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
export SCHEMAVER=2
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vSCHEMA=$SCHEMA -vID=$SCHEMAVER -vMETHOD='JSON_TABLE' -f $YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/${FILENAME}1.json to_user=\"$SCHEMA$SCHEMAVER\" log_file=$IMPORTLOG
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -q -vSCHEMA=$SCHEMA -vID1=1 -vID2=$SCHEMAVER -vMETHOD=$YADAMU_PARSER/ -f $YADAMU_SCRIPT_ROOT/sql/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
export FILENAME=jsonExample
export SCHEMA=jtest
export SCHEMAVER=1
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vSCHEMA=$SCHEMA -vID=$SCHEMAVER -vMETHOD=$YADAMU_PARSER/ -f $YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_INPUT_PATH/$FILENAME.json to_user=\"$SCHEMA$SCHEMAVER\" log_file=$IMPORTLOG
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
export SCHEMAVER=2
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vSCHEMA=$SCHEMA -vID=$SCHEMAVER -vMETHOD='JSON_TABLE' -f $YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_DB_ROOT/node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/${FILENAME}1.json to_user=\"$SCHEMA$SCHEMAVER\" log_file=$IMPORTLOG
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -q -vSCHEMA=$SCHEMA -vID1=1 -vID2=$SCHEMAVER -vMETHOD=$YADAMU_PARSER/ -f $YADAMU_SCRIPT_ROOT/sql/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$DB_DBNAME file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE log_file=$EXPORTLOG
node $YADAMU_HOME/utilities/node/compareFileSizes $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH
node $YADAMU_HOME/utilities/node/compareArrayContent $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH false