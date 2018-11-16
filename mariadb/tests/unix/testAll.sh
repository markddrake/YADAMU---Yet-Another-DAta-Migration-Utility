cls
. env/setEnvironment.sh
mkdir logs
rmdir /s /q $LOGDIR
mkdir -p $LOGDIR
mkdir -p $LOGDIR/install
export MODE=DATA_ONLY
. windows/clone_JSON.sh
. windows/clone_MYSQL.sh
. windows/clone_Oracle.sh 
. windows/clone_MSSQL_ALL.sh
. windows/clone_MSSQL.shun
export MODE=DDL_AND_DATA
. windows/clone_Oracle.sh