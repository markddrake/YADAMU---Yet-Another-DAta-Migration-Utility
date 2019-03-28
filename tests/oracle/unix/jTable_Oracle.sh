if [ -z ${MODE+x} ]; then export MODE=DATA_ONLY;fi
export YADAMU_TARGET=`basename $PWD`
export YADAMU_TARGET=$YADAMU_TARGET/$MODE/jTable
export YADAMU_PARSER=RDBMS
. ../unix/initialize.sh $(readlink -f "$0")
export YADAMU_INPUT_PATH=${YADAMU_INPUT_PATH:0:-7}
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_DB_ROOT/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
export SCHEMAVER=1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SCRIPT_ROOT/sql/RECREATE_ORACLE_ALL.sql $YADAMU_LOG_PATH $SCHEMAVER 
sh $YADAMU_SCRIPT_ROOT/unix/jTableImport_Oracle.sh $YADAMU_INPUT_PATH $SCHEMAVER ""
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SCRIPT_ROOT/sql/COMPARE_ORACLE_ALL.sql $YADAMU_LOG_PATH "" $SCHEMAVER $YADAMU_PARSER $MODE
sh $YADAMU_SCRIPT_ROOT/unix/export_Oracle.sh $YADAMU_OUTPUT_PATH $SCHEMAVER $SCHEMAVER $MODE
export SCHEMAVER=2
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SCRIPT_ROOT/sql/RECREATE_ORACLE_ALL.sql $YADAMU_LOG_PATH $SCHEMAVER 
sh $YADAMU_SCRIPT_ROOT/unix/jTableImport_Oracle.sh $YADAMU_OUTPUT_PATH $SCHEMAVER 1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SCRIPT_ROOT/sql/COMPARE_ORACLE_ALL.sql $YADAMU_LOG_PATH 1 $SCHEMAVER $YADAMU_PARSER $MODE
sh $YADAMU_SCRIPT_ROOT/unix/export_Oracle.sh $YADAMU_OUTPUT_PATH $SCHEMAVER $SCHEMAVER $MODE
node $YADAMU_HOME/utilities/node/compareFileSizes $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH
node $YADAMU_HOME/utilities/node/compareArrayContent $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH false