# Run from YADAMU_HOME
export YADAMU_HOME=`pwd`
export YADAMU_TEST_HOME=$YADAMU_HOME/tests
export YADAMU_WORK_ID=export
export YADAMU_WORK_ROOT=$YADAMU_HOME/work/$YADAMU_WORK_ID
if [ ! -e $YADAMU_WORK_ROOT ]; then mkdir $YADAMU_WORK_ROOT; fi
export YADAMU_LOG_ROOT=$YADAMU_WORK_ROOT/logs
source $YADAMU_TEST_HOME/unix/initializeLogging.sh
source $YADAMU_TEST_HOME/unix/installYadamu.sh
source $YADAMU_TEST_HOME/unix/createOutputFolders.sh $YADAMU_HOME
echo ":setvar ID ''" > setvars.sql
export SQLCMDINI=setvars.sql
export MSSQL_SCHEMA=AdventureWorksAll
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_HOME/tests/mssql/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/MSSQL_RECREATE_SCHEMA.log
node $YADAMU_TEST_HOME/node/testHarneunset SQLCMDINI
rm setvars.sql
ss CONFIG=$YADAMU_TEST_HOME/$YADAMU_WORK_ID/config.json >$YADAMU_LOG_PATH/$YADAMU_WORK_ID.log
