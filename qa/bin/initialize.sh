# $1 is the path to the script that initiated this process
# $2 is the path to the script that called this script.
# $3 is the RDBMS name required to calculate the location of input and output folders.
if [ -z ${NODE_NO_WARNINGS+x} ]; then export NODE_NO_WARNINGS=1; fi
if [ -z ${YADAMU_HOME+x} ]; then export YADAMU_HOME=`pwd`; fi
if [ -z ${MODE+x} ]; then  export MODE=DATA_ONLY; fi
#for %%I in (%~dp1..) do set YADAMU_TARGET=%%~nxI
export YADAMU_TARGET=$(basename "($(dirname "$(dirname "$1")")")
#for %%I in (%~dp2..) do set YADAMU_DB=%%~nxI
export YADAMU_DB=$(basename "$(dirname "$(dirname "$2")")")
export YADAMU_SOURCE=$3
if [ "${YADAMU_SOURCE: -1}" == "/" ]; then export YADAMU_SOURCE=$YADAMU_SOURCE$MODE; fi;
export YADAMU_TESTNAME=$4
export YADAMU_SHARED_DIR=$(dirname "${BASH_SOURCE[0]}") 
export YADAMU_BIN=$YADAMU_HOME/app/YADAMU/common
export YADAMU_QA_BIN=$YADAMU_HOME/app/YADAMU_QA/utilities/node
export YADAMU_ENV_PATH=$YADAMU_HOME/app/install/$YADAMU_TARGET
export YADAMU_SQL_PATH=$YADAMU_HOME/qa/sql/$YADAMU_DB
export YADAMU_SCRIPT_PATH=$(dirname "$2")
# JSON Files are in {YADAMU_HOME}/JSON/{RDBMS} or for Oracle {YADAMU_HOME/JSON/{SOURCE}/{MODE}
export YADAMU_JSON_BASE=$YADAMU_HOME/JSON
export YADAMU_INPUT_PATH=$YADAMU_JSON_BASE/$YADAMU_SOURCE
if [ $YADAMU_SOURCE = "oracle" ]; then export YADAMU_INPUT_PATH=$YADAMU_JSON_BASE/$YADAMU_TARGET/$MODE; fi
export YADAMU_TEST_BASE=$YADAMU_HOME/results
if [ -z ${YADAMU_TESTNAME+x} ]; then export YADAMU_TEST_BASE=$YADAMU_TEST_BASE/$YADAMU_TESTNAME; fi
if [ ! -e $YADAMU_TEST_BASE ]; then mkidr -p $YADAMU_TEST_BASE; fi
export YADAMU_TEST_BASE=$YADAMU_TEST_BASE/$YADAMU_TARGET
if [ ! -e $YADAMU_TEST_BASE ]; then source $YADAMU_SHARED_DIR/createOutputFolders.sh $YADAMU_TEST_BASE; fi
export YADAMU_OUTPUT_PATH=$YADAMU_TEST_BASE/JSON/$YADAMU_SOURCE
if [ -e $YADAMU_OUTPUT_PATH ]; then rm -rf $YADAMU_OUTPUT_PATH; fi
mkdir -p $YADAMU_OUTPUT_PATH
export YADAMU_LOG_ROOT=$YADAMU_HOME/log
. $YADAMU_SHARED_DIR/initializeLogging.sh $YADAMU_TESTNAME
. $YADAMU_ENV_PATH/env/dbConnection.sh
export | grep YADAMU