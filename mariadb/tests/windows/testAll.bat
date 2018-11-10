@set TNS=%~1
cls
mkdir logs
call windows\clone_JSON.bat
call windows\clone_MYSQL.bat
call windows\clone_Oracle.bat %TNS%
call windows\clone_MSSQL.bat
call windows\clone_MSSQL_ALL.bat
set RESULTS=jSax.log
dir JSON\MSSQL\*.json > logs\%RESULTS%
dir JSON\MariaDB\*.json >> logs\%RESULTS%
dir JSON\JSON\*.json  >> logs\%RESULTS%
dir JSON\%TNS%\*.json >> logs\%RESULTS%