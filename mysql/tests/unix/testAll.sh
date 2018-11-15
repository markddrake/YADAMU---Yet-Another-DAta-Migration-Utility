cls
. env/setEnvironment.bat
mkdir logs
rmdir /s /q $LOGDIR
mkdir -p $LOGDIR
mkdir -p $LOGDIR/install
. windows/export_Master.bat 
export MODE=DATA_ONLY
. windows/clone_JSON.bat
. windows/clone_MYSQL.bat
. windows/clone_Oracle.bat 
. windows/clone_MSSQL_ALL.bat
. windows/clone_MSSQL.bat
. windows/clone_JSON_jTable.bat
. windows/clone_MYSQL_jTable.bat
. windows/clone_Oracle_jTable.bat
. windows/clone_MSSQL_ALL_jTable.bat
. windows/clone_MSSQL_jTable.bat
export MODE=DDL_AND_DATA
. windows/clone_Oracle.bat 
. windows/clone_Oracle_jTable.bat 