clear
mkdir -p logs
export TNS=$1
. ./unix/export_Master.sh
. ./unix/clone_JSON.sh
. ./unix/clone_MYSQL.sh
. ./unix/clone_Oracle.sh $TNS
. ./unix/clone_MSSQL.sh
. ./unix/clone_MSSQL_ALL.sh
export RESULTS=jSax.log
ls -l JSON/MSSQL/*.json > logs/$RESULTS
ls -l JSON/MYSQL/*.json >> logs/$RESULTS
ls -l JSON/JSON/*.json  >> logs/$RESULTS
ls -l JSON/$TNS/*.json >> logs/$RESULTS
. ./unix/clone_JSON_jTable.sh
. ./unix/clone_MYSQL_jTable.sh
. ./unix/clone_Oracle_jTable.sh $TNS
. ./unix/clone_MSSQL_ALL_jTable.sh
export RESULTS=jTable.log
ls -l JSON/MSSQL/*.json > logs/$RESULTS
ls -l JSON/MYSQL/*.json >> logs/$RESULTS
ls -l JSON/JSON/*.json  >> logs/$RESULTS
ls -l JSON/$TNS/*.json >> logs/$RESULTS
