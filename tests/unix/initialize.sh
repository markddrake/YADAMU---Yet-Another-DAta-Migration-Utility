export YADAMU_TEST_ROOT=`pwd`
export YADAMU_WORK_ID=$(basename "$YADAMU_TEST_ROOT")
export YADAMU_SCRIPT_ROOT=$(dirname "$(dirname "$1")")
if [ -z ${MODE+x} ]; then  export MODE=DATA_ONLY; fi
if [ -z ${YADAMU_HOME+x} ]; then export YADAMU_HOME=$(dirname "$(dirname "$YADAMU_SCRIPT_ROOT")"); fi
YADAMU_DB=`basename $YADAMU_SCRIPT_ROOT`
export YADAMU_DB_ROOT=$YADAMU_HOME/$YADAMU_DB
export YADAMU_JSON_ROOT=$YADAMU_HOME/JSON
export YADAMU_INPUT_PATH=$YADAMU_JSON_ROOT/$YADAMU_TARGET
export YADAMU_WORK_ROOT=$YADAMU_HOME/work/$YADAMU_WORK_ID
export | grep YADAMU
if [ ! -e $YADAMU_WORK_ROOT ]; then mkdir $YADAMU_WORK_ROOT; fi
export YADAMU_LOG_ROOT=$YADAMU_WORK_ROOT/logs
. $YADAMU_HOME/tests/unix/initializeLogging.sh
export YADAMU_OUTPUT_PATH=$YADAMU_WORK_ROOT/JSON
if [ ! -e $YADAMU_OUTPUT_PATH ]; then sh $YADAMU_HOME/tests/unix/createOutputFolders.sh $YADAMU_WORK_ROOT; fi
export YADAMU_OUTPUT_PATH=$YADAMU_OUTPUT_PATH/$YADAMU_TARGET
if [ -e $YADAMU_OUTPUT_PATH ]; then rm -rf $YADAMU_OUTPUT_PATH; fi
mkdir $YADAMU_OUTPUT_PATH
. env/dbConnection.sh