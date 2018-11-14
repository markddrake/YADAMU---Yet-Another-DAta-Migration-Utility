@set TNS=%~1
cls
mkdir logs
@set LOGFILE=logs\Summary.log
del %LOGFILE%
call windows\export_Master.bat
call windows\clone_JSON.bat
call windows\clone_MYSQL.bat
call windows\clone_Oracle.bat %TNS%
call windows\clone_MSSQL.bat
call windows\clone_MSSQL_ALL.bat
set RESULTS=jSax.log
dir JSON\MSSQL\*.json > logs\%RESULTS%
dir JSON\MYSQL\*.json >> logs\%RESULTS%
dir JSON\JSON\*.json  >> logs\%RESULTS%
dir JSON\%TNS%\*.json >> logs\%RESULTS%
call windows\clone_JSON_jTable.bat
call windows\clone_MYSQL_jTable.bat
call windows\clone_Oracle_jTable.bat %TNS%
call windows\clone_MSSQL_jTable.bat
call windows\clone_MSSQL_ALL_jTable.bat
set RESULTS=jTable.log
dir JSON\MSSQL\*.json > logs\%RESULTS%
dir JSON\MYSQL\*.json >> logs\%RESULTS%
dir JSON\JSON\*.json  >> logs\%RESULTS%
dir JSON\%TNS%\*.json >> logs\%RESULTS%
type %LOGFILE%