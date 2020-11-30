source $YADAMU_HOME/src/install/mssql/env/dbConnection.sh
export MSSQL_SCHEMA=AdventureWorksAll
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_QA_HOME/sql/mssql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/mssql/RECREATE_SCHEMA.log
