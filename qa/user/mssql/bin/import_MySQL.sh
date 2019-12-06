source qa/bin/initialize.sh $BASH_SOURCE[0] $BASH_SOURCE[0] mysql import
export YADAMU_PARSER="CLARINET"
export FILENAME=sakila
export SCHEMA=sakila
export SCHEMAVER=1
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_INPUT_PATH/$FILENAME.json to_user=\"dbo\" mode=$MODE  log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE log_file=$YADAMU_EXPORT_LOG
export SCHEMAVER=2
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME1.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
export ID1=1
export ID2=$SCHEMAVER
export METHOD=$YADAMU_PARSER
export DATETIME_PRECISION=9
export SPATIAL_PRECISION=18
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE log_file=$YADAMU_EXPORT_LOG
export FILENAME=jsonExample
export SCHEMA=jtest
export SCHEMAVER=1
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_INPUT_PATH/$FILENAME.json to_user=\"dbo\" mode=$MODE  log_file=$YADAMU_IMPORT_LOG
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE log_file=$YADAMU_EXPORT_LOG
export SCHEMAVER=2
export MSSQL_SCHEMA="$SCHEMA$SCHEMAVER"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
node $YADAMU_BIN/import --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME1.json to_user=\"dbo\" mode=$MODE log_file=$YADAMU_IMPORT_LOG
export ID1=1
export ID2=$SCHEMAVER
export METHOD=$YADAMU_PARSER
export DATETIME_PRECISION=9
export SPATIAL_PRECISION=18
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
node $YADAMU_BIN/export --rdbms=$YADAMU_DB --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA$SCHEMAVER file=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMAVER.json owner=\"dbo\" mode=$MODE log_file=$YADAMU_EXPORT_LOG
node $YADAMU_QA_BIN/compareFileSizes $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH
node $YADAMU_QA_BIN/compareArrayContent $YADAMU_LOG_PATH $YADAMU_INPUT_PATH $YADAMU_OUTPUT_PATH false