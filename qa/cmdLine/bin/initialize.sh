# $1 is the path to the script that initiated this process
# $2 is the path to the script that called this script.
# $3 is the RDBMS name required to calculate the location of input and output folders.
if [ -z ${NODE_NO_WARNINGS+x} ]; then export NODE_NO_WARNINGS=1; fi
if [ -z ${YADAMU_HOME+x} ]; then export YADAMU_HOME=`pwd`; fi
if [ -z ${MODE+x} ]; then  export MODE=DATA_ONLY; fi
export YADAMU_BIN=$3
export YADAMU_OPERATION=$4
export YADAMU_TESTNAME="${YADAMU_TESTNAME:-${$5:-$4}}"
# if [ ! -z ${5+x} ]; then  export YADAMU_TESTNAME=$5; else export YADAMU_TESTNAME=$4; fi
export YADAMU_DATABASE=$(basename "($(dirname "$(dirname "$1")")")
export YADAMU_VENDOR=$(basename "$(dirname "$(dirname "$2")")")
export YADAMU_SHARED_PATH=qa/bin
export YADAMU_SCRIPT_PATH=$(dirname "$2")
export YADAMU_BIN=$YADAMU_HOME/bin
export YADAMU_LOG_ROOT=$YADAMU_HOME/log
source $YADAMU_SHARED_PATH/initializeLogging.sh $YADAMU_TESTNAME
export YADAMU_QA_JSPATH=$YADAMU_HOME/utilities/node
export YADAMU_ENV_PATH=$YADAMU_HOME/src/install/$YADAMU_DATABASE
source $YADAMU_ENV_PATH/env/dbConnection.sh
export YADAMU_SQL_PATH=$YADAMU_HOME/qa/sql/$YADAMU_VENDOR
export YADAMU_TEST_OUTPUT=$YADAMU_HOME/$YADAMU_TEST_FOLDER
# JSON Files are in {TEST_OUTPUT}\JSON\{DATABASE} or for Oracle {TEST_OUTPUT\JSON\{DATABASE}\{MODE}
export YADAMU_EXPORT_ROOT=$YADAMU_TEST_OUTPUT/JSON
export YADAMU_EXPORT_PATH=$YADAMU_EXPORT_ROOT/$YADAMU_DATABASE
if [ ! -e $YADAMU_EXPORT_PATH ]; then mkdir -p $YADAMU_EXPORT_PATH; fi
export YADAMU_IMPORT_MSSQL=$YADAMU_EXPORT_ROOT/$YADAMU_MSSQL_PATH
export YADAMU_IMPORT_MYSQL=$YADAMU_EXPORT_ROOT/$YADAMU_MYSQL_PATH
export YADAMU_IMPORT_ORACLE=$YADAMU_EXPORT_ROOT/$YADAMU_ORACLE_PATH/$MODE
# {TEST_OUTPUT}
export YADAMU_OUTPUT_PATH=$YADAMU_TEST_OUTPUT
# {TEST_OUTPUT}\{DATABASE}
export YADAMU_OUTPUT_PATH=$YADAMU_OUTPUT_PATH/$YADAMU_DATABASE
# {TEST_OUTPUT}\{DATABASE}\{MODE}
export YADAMU_OUTPUT_PATH=$YADAMU_OUTPUT_PATH/$MODE
# {TEST_OUTPUT}\{DATABASE}\{MODE}{TESTNAME}
if [ ! -z ${YADAMU_OPERATION+x} ]; then export YADAMU_OUTPUT_PATH=$YADAMU_OUTPUT_PATH/$YADAMU_OPERATION; fi
if [ ! -e $YADAMU_OUTPUT_PATH ]; then mkdir -p $YADAMU_OUTPUT_PATH; fi
printenv | grep "^YADAMU_" | sort