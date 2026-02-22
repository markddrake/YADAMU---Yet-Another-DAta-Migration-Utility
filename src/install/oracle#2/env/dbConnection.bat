@if defined ORACLE18C_USER (set DB_USER=%ORACLE18C_USER%) else (set DB_USER=system)
@if defined ORACLE18C_PWD (set DB_PWD=%ORACLE18C_PWD%) else (set DB_PWD=oracle)
@if defined ORACLE18C (set DB_CONNECTION=%ORACLE18C%) else (set DB_CONNECTION=ORCL)
