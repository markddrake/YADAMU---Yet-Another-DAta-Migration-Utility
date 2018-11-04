@set TNS=%~1
cls
call windows\export_master.bat
call windows\clone_JSON.bat
call windows\clone_MYSQL.bat
call windows\clone_oracle.bat %TNS%
call windows\clone_MSSQL.bat
call windows\clone_MSSQL_ALL.bat
set RESULTS=jSax.log
dir JSON\MSSQL\*.json > logs\%RESULTS%
dir JSON\MYSQL\*.json >> logs\%RESULTS%
dir JSON\JSON\*.json  >> logs\%RESULTS%
dir JSON\%TNS%\*.json >> logs\%RESULTS%
call windows\clone_JSON_jTable.bat
call windows\clone_MYSQL_jTable.bat
call windows\clone_oracle_jTable.bat %TNS%
call windows\clone_MSSQL_jTable.bat
call windows\clone_MSSQL_ALL_jTable.bat
set RESULTS=jTable.log
dir JSON\MSSQL\*.json > logs\%RESULTS%
dir JSON\MYSQL\*.json >> logs\%RESULTS%
dir JSON\JSON\*.json  >> logs\%RESULTS%
dir JSON\%TNS%\*.json >> logs\%RESULTS%
