@if defined MSSQL22_USER (set DB_USER=%MSSQL22_USER%) else (set DB_USER=sa)
@if defined MSSQL22_PWD (set DB_PWD=%MSSQL22_PWD%) else (set DB_PWD=oracle#1)
@if defined MSSQL22_HOST (set DB_HOST=%MSSQL22_HOST%) else (set DB_HOST=localhost)
@if defined MSSQL22_PORT (set DB_PORT=%MSSQL22_PORT%) else (set DB_PORT=1433)
@if defined MSSQL22_DBNAME (set DB_DBNAME=%MSSQL22_DBNAME%) else (set DB_DBNAME=master)
