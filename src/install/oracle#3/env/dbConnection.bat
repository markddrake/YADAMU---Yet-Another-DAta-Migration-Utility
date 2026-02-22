@if defined ORACLE12C_USER (set DB_USER=%ORACLE12C_USER%) else (set DB_USER=system)
@if defined ORACLE12C_PWD (set DB_PWD=%ORACLE12C_PWD%) else (set DB_PWD=oracle)
@if defined ORACLE12C (set DB_CONNECTION=%ORACLE12C%) else (set DB_CONNECTION=ORCL)
