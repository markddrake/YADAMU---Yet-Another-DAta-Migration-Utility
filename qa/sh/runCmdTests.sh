START_TIME=`date +%s`
export YADAMU_DB=$(basename "$1")
export YADAMU_SCRIPT_ROOT=$(dirname "$1") 
export MODE=DATA_ONLY
source $YADAMU_SCRIPT_ROOT/import_MYSQL.bat
source $YADAMU_SCRIPT_ROOT/import_Oracle.bat 
source $YADAMU_SCRIPT_ROOT/import_MSSQL.bat
if [ -e $YADAMU_SCRIPT_ROOT/upload_MySQL.bat ]; then source $YADAMU_SCRIPT_ROOT/upload_MYSQL.bat; fi
if [ -e $YADAMU_SCRIPT_ROOT/upload__MSSQL.bat ]; then source %YADAMU_SCRIPT_ROOT/upload_MSSQL.bat; fi
if [ -e $YADAMU_SCRIPT_ROOT/upload_Oracle.bat ]; then source $YADAMU_SCRIPT_ROOT/upload_Oracle.bat; fi
export MODE=DDL_AND_DATA
source $YADAMU_SCRIPT_ROOT/import_Oracle.bat 
if [ -e $YADAMU_SCRIPT_ROOT/upload__Oracle.bat ]; then source $YADAMU_SCRIPT_ROOT/upload_Oracle.bat; fi 
END_TIME=`date +%s`
ELAPSED_TIME=$((END_TIME-START_TIME))
echo "Elapsed time: $ELAPSED_TIMEs. Log Files written to $YADAMU_LOG_PATH."