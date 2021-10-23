# Run from YADAMU_HOME
$ENV:YADAMU_TASK=$1
Remove-Item Env:YADAMU_LOG_PATH
$ENV:YADAMU_LOG_ROOT=$ENV:YADAMU_HOME/log
source $ENV:YADAMU_QA_HOME/bin/initializeLogging.sh $ENV:YADAMU_TEST
if [ -z ${NODE_NO_WARNINGS+x} ]; then $ENV:NODE_NO_WARNINGS=1; fi
if [ -e $ENV:YADAMU_HOME/log/$ENV:YADAMU_TASK.log ]; then rm $ENV:YADAMU_HOME/log/$ENV:YADAMU_TASK.log; fi
touch  $ENV:YADAMU_LOG_PATH/$ENV:YADAMU_TASK.log
node $ENV:YADAMU_HOME/src/YADAMU_QA/common/node/test.js CONFIG=$ENV:YADAMU_QA_HOME/regression/$ENV:YADAMU_TASK.json EXCEPTION_FOLDER=$ENV:YADAMU_LOG_PATH 2>&1 | tee $ENV:YADAMU_LOG_PATH/$ENV:YADAMU_TASK.log 