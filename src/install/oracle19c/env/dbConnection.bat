@if defined ORACLE19C_USER (set DB_USER=%ORACLE19C_USER%) else (set DB_USER=system)
@if defined ORACLE19C_PWD (set DB_PWD=%ORACLE19C_PWD%) else (set DB_PWD=oracle)
@if defined ORACLE19C (set DB_CONNECTION=%ORACLE19C%) else (set DB_CONNECTION=ORCL)
