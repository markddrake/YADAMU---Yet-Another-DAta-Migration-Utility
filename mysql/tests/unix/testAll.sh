clear
export TNS=$1
. ./unix/export_master.sh
. ./unix/clone_JSON.sh
. ./unix/clone_MYSQL.sh
. ./unix/clone_oracle.sh $TNS
. ./unix/clone_MSSQL.sh
. ./unix/clone_MSSQL_ALL.sh
export RESULTS=jSax.log
dir JSON/MSSQL/*.json > logs/$RESULTS
dir JSON/MYSQL/*.json >> logs/$RESULTS
dir JSON/JSON/*.json  >> logs/$RESULTS
dir JSON/$TNS/*.json >> logs/$RESULTS
. ./unix/clone_JSON_jTable.sh
. ./unix/clone_MYSQL_jTable.sh
. ./unix/clone_oracle_jTable.sh $TNS
. ./unix/clone_MSSQL_ALL_jTable.sh
export RESULTS=jTable.log
dir JSON/MSSQL/*.json > logs/$RESULTS
dir JSON/MYSQL/*.json >> logs/$RESULTS
dir JSON/JSON/*.json  >> logs/$RESULTS
dir JSON/$TNS/*.json >> logs/$RESULTS
