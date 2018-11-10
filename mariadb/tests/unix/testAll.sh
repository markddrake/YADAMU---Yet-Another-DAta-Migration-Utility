clear
mkdir -p logs
export TNS=$1
. ./unix/clone_JSON.sh
. ./unix/clone_MYSQL.sh
. ./unix/clone_Oracle.sh $TNS
. ./unix/clone_MSSQL.sh
. ./unix/clone_MSSQL_ALL.sh
export RESULTS=jSax.log
ls -l JSON/MSSQL/*.json > logs/$RESULTS
ls -l JSON/MariaDB/*.json >> logs/$RESULTS
ls -l JSON/JSON/*.json  >> logs/$RESULTS
ls -l JSON/$TNS/*.json >> logs/$RESULTS