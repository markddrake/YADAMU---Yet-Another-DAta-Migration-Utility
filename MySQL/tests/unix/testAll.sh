clear
export TNS=%1
mkdir -p logs
mkdir -p JSON
./unix/export_master.sh
./unix/clone_JSON_jSax.sh
./unix/clone_MYSQL_jSax.sh
./unix/clone_oracle_jSax.sh $TNS
./unix/clone_MSSQL_jSax.sh
./unix/clone_MSSQL_ALL_jSax.sh
export RESULTS=jSax.log
ls JSON/MSSQL/*.json > logs/$RESULTS
ls JSON/MYSQL/*.json >> logs/$RESULTS
ls JSON/JSON/*.json  >> logs/$RESULTS
ls JSON/$TNS/*.json >> logs/$RESULTS
./unix/clone_JSON_jTable.sh
./unix/clone_MYSQL_jTable.sh
./unix/clone_oracle_jTable.sh $TNS
./unix/clone_MSSQL_ALL_jTable.sh
export RESULTS=jTable.log
ls JSON/MSSQL/*.json > logs/$RESULTS
ls JSON/MYSQL/*.json >> logs/$RESULTS
ls JSON/JSON/*.json  >> logs/$RESULTS
ls JSON/$TNS/*.json >> logs/$RESULTS