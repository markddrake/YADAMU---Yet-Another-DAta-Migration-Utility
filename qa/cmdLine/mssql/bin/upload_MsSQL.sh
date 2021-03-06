source qa/cmdLine/bin/initialize.sh $1 $BASH_SOURCE[0] mssql upload $YADAMU_TESTNAME
export YADAMU_PARSER="SQL"
export SCHEMA_VERSION=1
export ID=$SCHEMA_VERSION
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
source $YADAMU_SCRIPT_PATH/upload_operations_MsSQL.sh $YADAMU_EXPORT_PATH $SCHEMA_VERSION ""
export ID1=" "
export ID2=$SCHEMA_VERSION
export METHOD=$YADAMU_PARSER
export DATETIME_PRECISION=9
export SPATIAL_PRECISION=18
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/COMPARE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
source $YADAMU_SCRIPT_PATH/export_operations_MsSQL.sh $YADAMU_OUTPUT_PATH $SCHEMA_VERSION $SCHEMA_VERSION
export PRIOR_VERSION=$SCHEMA_VERSION
let "SCHEMA_VERSION+=1"
export ID=$SCHEMA_VERSION
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
source $YADAMU_SCRIPT_PATH/upload_operations_MsSQL.sh $YADAMU_OUTPUT_PATH $SCHEMA_VERSION $PRIOR_VERSION
export ID1=$PRIOR_VERSION
export ID2=$SCHEMA_VERSION
export METHOD=$YADAMU_PARSER
export DATETIME_PRECISION=9
export SPATIAL_PRECISION=18
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/COMPARE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log
source $YADAMU_SCRIPT_PATH/export_operations_MsSQL.sh $YADAMU_OUTPUT_PATH $SCHEMA_VERSION $SCHEMA_VERSION
export SCHEMA=ADVWRK
export SCHEMA_VERSION=1
export FILENAME=AdventureWorksALL
export MSSQL_SCHEMA="$SCHEMA$SCHEMA_VERSION"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
source $YADAMU_BIN/upload.sh --RDBMS=$YADAMU_VENDOR --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD --DATABASE=$SCHEMA$SCHEMA_VERSION FILE=$YADAMU_EXPORT_PATH/$FILENAME.json ENCRYPTION=false TO_USER=\"dbo\" LOG_FILE=$YADAMU_IMPORT_LOG  EXCEPTION_FOLDER=$YADAMU_LOG_PATH
source $YADAMU_BIN/export.sh --RDBMS=$YADAMU_VENDOR --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD --DATABASE=$SCHEMA$SCHEMA_VERSION FILE=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMA_VERSION.json ENCRYPTION=false FROM_USER=\"dbo\" MODE=$MODE LOG_FILE=$YADAMU_EXPORT_LOG  EXCEPTION_FOLDER=$YADAMU_LOG_PATH
export PRIOR_VERSION=$SCHEMA_VERSION
let "SCHEMA_VERSION+=1"
export MSSQL_SCHEMA="$SCHEMA$SCHEMA_VERSION"
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
source $YADAMU_BIN/upload.sh --RDBMS=$YADAMU_VENDOR --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD --DATABASE=$SCHEMA$SCHEMA_VERSION FILE=$YADAMU_OUTPUT_PATH/$FILENAME$PRIOR_VERSION.json ENCRYPTION=false TO_USER=\"dbo\" LOG_FILE=$YADAMU_IMPORT_LOG  EXCEPTION_FOLDER=$YADAMU_LOG_PATH
export ID1=$PRIOR_VERSION
export ID2=$SCHEMA_VERSION
export METHOD=$YADAMU_PARSER
export DATETIME_PRECISION=9
export SPATIAL_PRECISION=18
export DATABASE=$DB_DBNAME
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SQL_PATH/COMPARE_SCHEMA.sql >>$YADAMU_LOG_PATH/COMPARE_SCHEMA.log 
source $YADAMU_BIN/export.sh --RDBMS=$YADAMU_VENDOR --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD --DATABASE=$SCHEMA$SCHEMA_VERSION FILE=$YADAMU_OUTPUT_PATH/$FILENAME$SCHEMA_VERSION.json ENCRYPTION=false FROM_USER=\"dbo\" MODE=$MODE LOG_FILE=$YADAMU_EXPORT_LOG  EXCEPTION_FOLDER=$YADAMU_LOG_PATH
node $YADAMU_QA_JSPATH/compareFileSizes $YADAMU_LOG_PATH $YADAMU_EXPORT_PATH $YADAMU_OUTPUT_PATH
node --max_old_space_size=4096 $YADAMU_QA_JSPATH/compareArrayContent $YADAMU_LOG_PATH $YADAMU_EXPORT_PATH $YADAMU_OUTPUT_PATH false