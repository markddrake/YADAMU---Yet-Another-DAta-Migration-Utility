clear
. env/setEnvironment.sh
mkdir -p logs
rm -rf $LOGDIR
mkdir -p $LOGDIR
mkdir -p $LOGDIR/install
sh unix/export_Master.sh 
export MODE=DATA_ONLY
sh unix/clone_JSON.sh
sh unix/clone_MYSQL.sh
sh unix/clone_Oracle.sh
sh unix/clone_MSSQL_ALL.sh
sh unix/clone_MSSQL.sh
sh unix/clone_JSON_jTable.sh
sh unix/clone_MYSQL_jTable.sh
sh unix/clone_Oracle_jTable.sh
sh unix/clone_MSSQL_ALL_jTable.sh
sh unix/clone_MSSQL_jTable.sh
export MODE=DDL_AND_DATA
sh unix/clone_Oracle.sh
sh unix/clone_Oracle_jTable.sh