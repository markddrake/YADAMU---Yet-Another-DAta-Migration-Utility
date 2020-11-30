#1/bin/bash
START_TIME=`date +%s`
source qa/cmdLine/bin/initialize.sh $1 $BASH_SOURCE[0] oracle export $YADAMU_TESTNAME
rm -rf $YADAMU_EXPORT_PATH
export MODE=DDL_ONLY
export  YADAMU_EXPORT_TARGET=$YADAMU_EXPORT_PATH/$MODE
mkdir -p  $YADAMU_EXPORT_TARGET
if [ ! -e $YADAMU_EXPORT_TARGET ]; then mkdir -p $YADAMU_EXPORT_TARGET; fi
source $YADAMU_SCRIPT_PATH/export_operations_Oracle.sh $YADAMU_EXPORT_TARGET "" "" $MODE
export MODE=DATA_ONLY
export  YADAMU_EXPORT_TARGET=$YADAMU_EXPORT_PATH/$MODE
mkdir -p  $YADAMU_EXPORT_TARGET
if [ ! -e $YADAMU_EXPORT_TARGET ]; then mkdir -p $YADAMU_EXPORT_TARGET; fi
source $YADAMU_SCRIPT_PATH/export_operations_Oracle.sh $YADAMU_EXPORT_TARGET "" "" $MODE
export MODE=DDL_AND_DATA
export  YADAMU_EXPORT_TARGET=$YADAMU_EXPORT_PATH/$MODE
mkdir -p  $YADAMU_EXPORT_TARGET
if [ ! -e $YADAMU_EXPORT_TARGET ]; then mkdir -p $YADAMU_EXPORT_TARGET; fi
source $YADAMU_SCRIPT_PATH/export_operations_Oracle.sh $YADAMU_EXPORT_TARGET "" "" $MODE
END_TIME=`date +%s`
TOTAL_TIME=$((END_TIME-START_TIME))
ELAPSED_TIME=`date -d@$TOTAL_TIME -u +%H:%M:%S`
echo "Export ${YADAMU_DATABASE}. Elapsed time: ${ELAPSED_TIME}. Log Files written to ${YADAMU_LOG_PATH}."