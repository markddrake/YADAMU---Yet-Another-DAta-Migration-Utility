export YADAMU_HOME=`pwd`
export YADAMU_LOG_ROOT=$YADAMU_HOME/log
source $YADAMU_HOME/qa/sh/initializeLogging.sh usermode
export SESSION_LOG_PATH=$YADAMU_LOG_PATH
unset YADAMU_LOG_PATH
# Mode is set internally by the export_sample_datasets scripts
source qa/user/oracle19c/sh/export_sample_datasets.sh 1> $SESSION_LOG_PATH/export/export.log  2>&1
source qa/user/oracle18c/sh/export_sample_datasets.sh 1> $SESSION_LOG_PATH/export/export.log  2>&1
source qa/user/oracle12c/sh/export_sample_datasets.sh 1> $SESSION_LOG_PATH/export/export.log  2>&1
source qa/user/oracle11g/sh/export_sample_datasets.sh 1> $SESSION_LOG_PATH/export/export.log  2>&1
source qa/user/mssql/sh/export_sample_datasets.sh 1> $SESSION_LOG_PATH/export/export.log  2>&1
source qa/user/mysql/sh/export_sample_datasets.sh 1> $SESSION_LOG_PATH/export/export.log  2>&1
export MODE=DATA_ONLY
unset YADAMU_LOG_PATH
source qa/user/oracle19c/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/oracle19c.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/oracle18c/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/oracle18c.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/oracle12c/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/oracle12c.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/oracle11g/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/oracle11g.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/mssql/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/mssql.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/postgres/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/postgres.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/mysql/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/mysql.log 2>&1
unset YADAMU_LOG_PATH
source qa/user/mariadb/sh/runCmdTests.sh 1> $SESSION_LOG_PATH/mariadb.log 2>&1
