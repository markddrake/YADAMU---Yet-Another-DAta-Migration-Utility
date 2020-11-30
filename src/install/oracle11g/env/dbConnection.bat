@if defined ORACLE11G_USER (set DB_USER=%ORACLE11G_USER%) else (set DB_USER=system)
@if defined ORACLE11G_PWD (set DB_PWD=%ORACLE11G_PWD%) else (set DB_PWD=oracle)
@if defined ORACLE11G (set DB_CONNECTION=%ORACLE11G%) else (set DB_CONNECTION=ORCL)
