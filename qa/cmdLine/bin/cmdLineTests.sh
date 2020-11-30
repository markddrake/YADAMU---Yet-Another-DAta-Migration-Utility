#1/bin/bash
START_TIME=`date +%s`
export YADAMU_VENDOR=$(basename "$1")
export YADAMU_SCRIPT_ROOT=$(dirname "$1") 
export MODE=DATA_ONLY
source $YADAMU_SCRIPT_ROOT/import_MySQL.sh
source $YADAMU_SCRIPT_ROOT/import_Oracle.sh 
source $YADAMU_SCRIPT_ROOT/import_MsSQL.sh
if [ -e $YADAMU_SCRIPT_ROOT/upload_MsSQL.sh ]; then source $YADAMU_SCRIPT_ROOT/upload_MsSQL.sh; fi
if [ -e $YADAMU_SCRIPT_ROOT/upload_MySQL.sh ]; then source $YADAMU_SCRIPT_ROOT/upload_MySQL.sh; fi
if [ -e $YADAMU_SCRIPT_ROOT/upload_Oracle.sh ]; then source $YADAMU_SCRIPT_ROOT/upload_Oracle.sh; fi
export MODE=DDL_AND_DATA
source $YADAMU_SCRIPT_ROOT/import_Oracle.sh 
if [ -e $YADAMU_SCRIPT_ROOT/upload_Oracle.sh ]; then source $YADAMU_SCRIPT_ROOT/upload_Oracle.sh; fi 
END_TIME=`date +%s`
TOTAL_TIME=$((END_TIME-START_TIME))
ELAPSED_TIME=`date -d@$TOTAL_TIME -u +%H:%M:%S`
echo "${YADAMU_DATABASE}. Elapsed time: ${ELAPSED_TIME}. Log Files written to ${YADAMU_LOG_PATH}."