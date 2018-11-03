export TNS=$~1
caLL windows/export_master.sh
call windows/clone_JSON_jSax.sh
call windows/clone_MYSQL_jSax.sh
call windows/clone_oracle_jSax.sh $TNS$
call windows/clone_MSSQL_jSax.sh
call windows/clone_MSSQL_ALL_jSax.sh
set RESULTS=jSax.log
dir JSON/MSSQL/*.json > log/$RESULTS$
dir JSON/MYSQL/*.json >> log/$RESULTS$
dir JSON/JSON/*.json  >> log/$RESULTS$
dir JSON/$TNS/*.json >> log/$RESULTS$
call windows/clone_JSON_jTable.sh
call windows/clone_MYSQL_jTable.sh
call windows/clone_oracle_jTable.sh $TNS$
call windows/clone_MSSQL_ALL_jTable.sh
set RESULTS=jTable.log
dir JSON/MSSQL/*.json > log/$RESULTS$
dir JSON/MYSQL/*.json >> log/$RESULTS$
dir JSON/JSON/*.json  >> log/$RESULTS$
dir JSON/$TNS/*.json >> log/$RESULTS$
