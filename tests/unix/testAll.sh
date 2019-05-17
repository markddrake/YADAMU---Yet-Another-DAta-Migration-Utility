
EXPORT_NATIVE_SCHEMAS() 
{
cd $YADAMU_HOME/tests/oracle19c
. unix/export_Native_Schemas.sh 1> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/oracle18c
. unix/export_Native_Schemas.sh 1> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/oracle12c
. unix/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
export mode=DATA_ONLY
cd $YADAMU_HOME/tests/mysql
. unix/export_Native_Schemas.sh 1>> $MASTER_LOG_PATH/Export.log  2>&1
cd $YADAMU_HOME/tests/mssql
cp -r $YADAMU_HOME/testdata/mssql $YADAMU_HOME/JSON 1>> $MASTER_LOG_PATH/Export.log  2>&1
}

ORACLE19c()
{
cd $YADAMU_HOME/tests/oracle19c
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle19c.log 2>&1
}

ORACLE18c()
{
cd $YADAMU_HOME/tests/oracle18c
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle18c.log 2>&1
}

ORACLE12c()
{
cd $YADAMU_HOME/tests/oracle12c
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/Oracle12c.log  2>&1
}

MSSQL() 
{
cd $YADAMU_HOME/tests/mssql
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/MsSQL.log  2>&1
}

POSTGRES()
{
cd $YADAMU_HOME/tests/postgres
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/Postgres.log  2>&1
}

MYSQL() 
{
cd $YADAMU_HOME/tests/mysql
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/MySQL.log  2>&1
}

MARIADB() 
{
cd $YADAMU_HOME/tests/mariadb
unset YADAMU_LOG_PATH
. unix/runAllTests.sh 1> $MASTER_LOG_PATH/MariaDB.log  2>&1
}

export YADAMU_HOME=`pwd`
export YADAMU_LOG_ROOT=$YADAMU_HOME/work/logs
unset YADAMU_LOG_PATH
. $YADAMU_HOME/tests/unix/initializeLogging.sh
export MASTER_LOG_PATH=$YADAMU_LOG_PATH
EXPORT_NATIVE_SCHEMAS
ORACLE19c
ORACLE18c
ORACLE12c
MSSQL
POSTGRES
MYSQL
MARIADB
cd $YADAMU_HOME

