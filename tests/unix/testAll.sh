cls
export YADAMU_HOME=`pwd`
export YADAMU_LOG_ROOT=$YADAMU_HOME/work/logs
export YADAMU_LOG_PATH=
sh $YADAMU_HOME/tests/unix/initializeLogging.sh
export MASTER_LOG_PATH=$YADAMU_LOG_PATH
call :EXPORT_NATIVE_SCHEMAS
call :ORACLE18c
call :MSSQL
call :POSTGRES
call :MySQL
call :MARIADB
call :ORACLE12c
cd $YADAMU_HOME
exit /b

:EXPORT_NATIVE_SCHEMAS
cd $YADAMU_HOME/tests/oracle18c
call windows/export_Native_Schemas.sh 1> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/oracle12c
call windows/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
export mode=DATA_ONLY
cd $YADAMU_HOME/tests/mssql
call windows/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/mysql
call windows/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
exit /b

:ORACLE18c
cd $YADAMU_HOME/tests/oracle18c
export YADAMU_LOG_PATH=
call windows/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle18c.log 2>&1
exit /b

:POSTGRES
cd $YADAMU_HOME/tests/postgres
export YADAMU_LOG_PATH=
call windows/runAllTests.sh 1> $MASTER_LOG_PATH/Postgres.log  2>&1
exit /b

:MSSQL
cd $YADAMU_HOME/tests/mssql
export YADAMU_LOG_PATH=
call windows/runAllTests.sh 1> $MASTER_LOG_PATH/MsSQL.log  2>&1
exit /b

:MYSQL
cd $YADAMU_HOME/tests/mysql
export YADAMU_LOG_PATH=
call windows/runAllTests.sh 1> $MASTER_LOG_PATH/MySQL.log  2>&1
exit /b

:MARIADB
cd $YADAMU_HOME/tests/mariaDB
export YADAMU_LOG_PATH=
call windows/runAllTests.sh 1> $MASTER_LOG_PATH/MariaDB.log  2>&1
exit /b

:ORACLE12c
cd $YADAMU_HOME/tests/oracle12c
export YADAMU_LOG_PATH=
call windows/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle12c.log  2>&1
exit /b