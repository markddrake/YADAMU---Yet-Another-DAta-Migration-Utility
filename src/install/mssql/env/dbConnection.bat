@if defined MSSQL_HOST (set DB_USER=%MSSQL_HOST%) else (set DB_USER=sa
@if defined MSSQL_PWD (set DB_PWD=%MSSQL_PWD%) else (set DB_PWD=oracle#1
@if defined MSSQL_HOST (set DB_HOST=%MSSQL_HOST%) else (set DB_HOST=localhost
@if defined MSSQL_PORT (set DB_PORT=%MSSQL_PORT%) else (set DB_PORT=1433
@if defined MSSQL_DBNAME (set DB_DBNAME=%MSSQL_DBNAME%) else (set DB_DBNAME=master
