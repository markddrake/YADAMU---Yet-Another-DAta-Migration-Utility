#!/bin/bash
TESTSUITE_START_TIME=`date +%s`
if [ -z ${1+x} ]; then export YADAMU_SETTINGS=default; else export YADAMU_SETTINGS=$1; fi
export YADAMU_HOME=`pwd`
export YADAMU_QA_HOME=$YADAMU_HOME/qa
unset YADAMU_TIMESTAMP
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}") 
source $YADAMU_SCRIPT_DIR/../settings/$YADAMU_SETTINGS.sh
source $YADAMU_SCRIPT_DIR/runCmdLineTests.sh
END_TIME=`date +%s`
TOTAL_TIME=$((END_TIME-TESTSUITE_START_TIME))
ELAPSED_TIME=`date -d@$TOTAL_TIME -u +%H:%M:%S`
echo "Test Suite Elapsed time: ${ELAPSED_TIME}."