. qa/sh/initialize.sh $1 $BASH_SOURCE[0] mssql upload
export YADAMU_PARSER="SQL"
export SCHEMAVER=1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SQL_PATH/RECREATE_MSSQL_ALL.sql $YADAMU_LOG_PATH $SCHEMAVER 
source $YADAMU_SCRIPT_PATH/upload_operations_MsSQL.sh $YADAMU_INPUT_PATH $SCHEMAVER ""
source $YADAMU_SCRIPT_PATH/export_operations_MsSQL.sh $YADAMU_OUTPUT_PATH $SCHEMAVER $SCHEMAVER
export SCHEMAVER=2
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SQL_PATH/RECREATE_MSSQL_ALL.sql $YADAMU_LOG_PATH $SCHEMAVER 
source $YADAMU_SCRIPT_PATH/upload_operations_MsSQL.sh $YADAMU_OUTPUT_PATH $SCHEMAVER 1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SQL_PATH/COMPARE_MSSQL_ALL.sql $YADAMU_LOG_PATH 1 $SCHEMAVER $YADAMU_PARSER $MODE
source $YADAMU_SCRIPT_PATH/export_operations_MsSQL.sh $YADAMU_OUTPUT_PATH $SCHEMAVER $SCHEMAVER
export FILENAME=AdventureWorksALL
export SCHEMAVER=1
export SCHEMA=ADVWRK
export SCHEMAVER=1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH $SCHEMA$SCHEMAVER 
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION  file=$YADAMU_INPUT_PATH/$FILENAME.json to_user=\"$SCHEMA$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION  file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
export SCHEMAVER=2
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH $SCHEMA$SCHEMAVER 
node $YADAMU_BIN/upload --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION  file=$YADAMU_OUTPUT_PATH/${FILENAME}1.json to_user=\"$SCHEMA$SCHEMAVER\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_SQL_PATH/COMPARE_SCHEMA.sql $YADAMU_LOG_PATH $SCHEMA 1 $SCHEMAVER $YADAMU_PARSER $MODE
node $YADAMU_BIN/export --rdbms=$YADAMU_DB userid=$DB_USER/$DB_PWD@$DB_CONNECTION  file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"$SCHEMA$SCHEMAVER\" mode=$MODE  log_file=$YADAMU_EXPORT_LOG
node $YADAMU_QA_BIN/compareFileSizes $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH
node --max_old_space_size=4096 $YADAMU_QA_BIN/compareArrayContent $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH false