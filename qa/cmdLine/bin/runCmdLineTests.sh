export YADAMU_HOME=`pwd`
export YADAMU_LOG_ROOT=$YADAMU_HOME/log
export YADAMU_TESTNAME=cmdLine
export YADAMU_OUTPUT_PATH=$YADAMU_HOME/$YADAMU_TESTNAME
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Output written to: ${YADAMU_OUTPUT_PATH}"
if [ -e $YADAMU_OUTPUT_PATH ]; then rm -rf $YADAMU_OUTPUT_PATH; fi
mkdir -p $YADAMU_OUTPUT_PATH
source $YADAMU_HOME/qa/bin/initializeLogging.sh $YADAMU_TESTNAME
export YADAMU_LOG_FOLDER=$YADAMU_LOG_PATH
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Scripts logged to: ${YADAMU_LOG_PATH}"
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Yadamu log file: ${YADAMU_IMPORT_LOG}"
# Mode is set internally by the export_sample_datasets scripts
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting Oracle#1"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_oracle#1.log
source qa/cmdLine/oracle#1/bin/export_sample_datasets.sh     1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting Oracle#2"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_oracle#2.log
source qa/cmdLine/oracle#2/bin/export_sample_datasets.sh     1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting Oracle#3"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_oracle#3.log
source qa/cmdLine/oracle#3/bin/export_sample_datasets.sh     1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting Oracle#4"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_oracle#4.log
source qa/cmdLine/oracle#4/bin/export_sample_datasets.sh     1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting MsSQL Server#1"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_mssql#1.log
source qa/cmdLine/mssql#1/bin/export_sample_datasets.sh       1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting MsSQL Server#2"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_mssql#2.log
source qa/cmdLine/mssql#2/bin/export_sample_datasets.sh       1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Exporting MySQL"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/export_mysql.log
source qa/cmdLine/mysql/bin/export_sample_datasets.sh         1> $SHELL_LOG_FILE 2>&1
export MODE=DATA_ONLY
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing Oracle#1"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/oracle#1.log
source qa/cmdLine/oracle#1/bin/cmdLineTests.sh               1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing Oracle#2"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/oracle#2.log
source qa/cmdLine/oracle#2/bin/cmdLineTests.sh               1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing Oracle#3"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/oracle#3.log
source qa/cmdLine/oracle#3/bin/cmdLineTests.sh               1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing Oracle#4"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/oracle#4.log
source qa/cmdLine/oracle#4/bin/cmdLineTests.sh               1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing MsSQL#1"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/mssql#1.log
source qa/cmdLine/mssql#1/bin/cmdLineTests.sh                 1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing MsSQL#2"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/mssql#2.log
source qa/cmdLine/mssql#2/bin/cmdLineTests.sh                 1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing Postgres"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/postgres.log
source qa/cmdLine/postgres/bin/cmdLineTests.sh                1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing MySQL"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/mysql.log
source qa/cmdLine/mysql/bin/cmdLineTests.sh                   1> $SHELL_LOG_FILE 2>&1
export CURRRENT_TIMESTAMP=$(date --utc +%FT%H:%M:%S.%3N)
echo "$CURRRENT_TIMESTAMP: Testing MariaDB"
export SHELL_LOG_FILE=$YADAMU_LOG_PATH/mariadb.log
source qa/cmdLine/mariadb/bin/cmdLineTests.sh                 1> $SHELL_LOG_FILE 2>&1
