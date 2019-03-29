cls
export YADAMU_HOME=`pwd`
export YADAMU_LOG_ROOT=$YADAMU_HOME/work/logs
export YADAMU_LOG_PATH=
. $YADAMU_HOME/tests/unix/initializeLogging.sh
export MASTER_LOG_PATH=$YADAMU_LOG_PATH
. :EXPORT_NATIVE_SCHEMAS
. :ORACLE18c
. :MSSQL
. :POSTGRES
. :MySQL
. :MARIADB
. :ORACLE12c
cd $YADAMU_HOME
exit /b

:EXPORT_NATIVE_SCHEMAS
cd $YADAMU_HOME/tests/oracle18c
. windows/export_Native_Schemas.sh 1> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/oracle12c
. windows/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
export mode=DATA_ONLY
cd $YADAMU_HOME/tests/mssql
. windows/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/mysql
. windows/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
exit /b

:ORACLE18c
cd $YADAMU_HOME/tests/oracle18c
export YADAMU_LOG_PATH=
. windows/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle18c.log 2>&1
exit /b

:POSTGRES
cd $YADAMU_HOME/tests/postgres
export YADAMU_LOG_PATH=
. windows/runAllTests.sh 1> $MASTER_LOG_PATH/Postgres.log  2>&1
exit /b

:MSSQL
cd $YADAMU_HOME/tests/mssql
export YADAMU_LOG_PATH=
. windows/runAllTests.sh 1> $MASTER_LOG_PATH/MsSQL.log  2>&1
exit /b

:MYSQL
cd $YADAMU_HOME/tests/mysql
export YADAMU_LOG_PATH=
. windows/runAllTests.sh 1> $MASTER_LOG_PATH/MySQL.log  2>&1
exit /b

:MARIADB
cd $YADAMU_HOME/tests/mariaDB
export YADAMU_LOG_PATH=
. windows/runAllTests.sh 1> $MASTER_LOG_PATH/MariaDB.log  2>&1
exit /b

:ORACLE12c
cd $YADAMU_HOME/tests/oracle12c
export YADAMU_LOG_PATH=
. windows/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle12c.log  2>&1
exit /b