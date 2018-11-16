cls
. env/setEnvironment.sh
mkdir logs
rmdir /s /q $LOGDIR
mkdir -p $LOGDIR
mkdir -p $LOGDIR/install
. windows/export_Master.sh 
export MODE=DATA_ONLY
. windows/clone_JSON.sh
. windows/clone_MYSQL.sh
. windows/clone_Oracle.sh 
. windows/clone_MSSQL_ALL.sh
. windows/clone_MSSQL.sh
. windows/clone_JSON_jTable.sh
. windows/clone_MYSQL_jTable.sh
. windows/clone_Oracle_jTable.sh
. windows/clone_MSSQL_ALL_jTable.sh
. windows/clone_MSSQL_jTable.sh
export MODE=DDL_AND_DATA
. windows/clone_Oracle.sh 
. windows/clone_Oracle_jTable.sh 