# Run from YADAMU_HOME
export YADAMU_HOME=`pwd`
export YADAMU_TEST_HOME=$YADAMU_HOME/tests
export YADAMU_WORK_ID=dbRoundtrip
export YADAMU_WORK_ROOT=$YADAMU_HOME/work
if [ ! -e $YADAMU_WORK_ROOT ]; then mkdir $YADAMU_WORK_ROOT; fi
export YADAMU_LOG_ROOT=$YADAMU_WORK_ROOT/logs
source $YADAMU_TEST_HOME/unix/initializeLogging.sh
source $YADAMU_TEST_HOME/unix/installYadamu.sh
source $YADAMU_TEST_HOME/unix/createTestUsers.sh 1
source $YADAMU_TEST_HOME/unix/createTestUsers.sh 2
node $YADAMU_TEST_HOME/node/testHarness CONFIG=$YADAMU_TEST_HOME/$YADAMU_WORK_ID/config.json >$YADAMU_LOG_PATH/$YADAMU_WORK_ID.log