export YADAMU_HOME=`pwd`
export YADAMU_LOG_ROOT=%YADAMU_HOME%/log
call %YADAMU_HOME%/qa/bin/initializeLogging.sh usermode
export SESSION_LOG_PATH=%YADAMU_LOG_PATH%
unset YADAMU_LOG_PATH
# Mode is set internally by the export_sample_datasets scripts
call qa/user/oracle19c/bin/export_sample_datasets.sh 1> %SESSION_LOG_PATH%/export/export.log  2>&1
call qa/user/oracle18c/bin/export_sample_datasets.sh 1> %SESSION_LOG_PATH%/export/export.log  2>&1
call qa/user/oracle12c/bin/export_sample_datasets.sh 1> %SESSION_LOG_PATH%/export/export.log  2>&1
call qa/user/oracle11g/bin/export_sample_datasets.sh 1> %SESSION_LOG_PATH%/export/export.log  2>&1
call qa/user/mssql/bin/export_sample_datasets.sh 1> %SESSION_LOG_PATH%/export/export.log  2>&1
call qa/user/mysql/bin/export_sample_datasets.sh 1> %SESSION_LOG_PATH%/export/export.log  2>&1
export MODE=DATA_ONLY
unset YADAMU_LOG_PATH
call qa/user/oracle19c/bin/runCmdTests.sh 1> %SESSION_LOG_PATH%/oracle19c.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/oracle18c/bin/runCmdTests.sh 1> %SESSION_LOG_PATH%/oracle18c.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/oracle12c/bin/runCmdTests.sh 1> %SESSION_LOG_PAT%H/oracle12c.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/oracle11g/bin/runCmdTests.sh 1> %SESSION_LOG_PAT%H/oracle11g.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/mssql/bin/runCmdTests.sh 1> %SESSION_LOG_PATH%/mssql.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/postgres/bin/runCmdTests.sh 1> %SESSION_LOG_PATH%/postgres.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/mysql/bin/runCmdTests.sh 1> %SESSION_LOG_PATH%/mysql.log 2>&1
unset YADAMU_LOG_PATH
call qa/user/mariadb/bin/runCmdTests.sh 1> %SESSION_LOG_PATH%/mariadb.log 2>&1
