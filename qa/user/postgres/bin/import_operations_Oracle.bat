@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\HR%VER%.json to_user=\"HR%SCHEMAVER%\" log_file=%YADAMU_IMPORT_LOG% mode=%MODE%
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\SH%VER%.json to_user=\"SH%SCHEMAVER%\" log_file=%YADAMU_IMPORT_LOG% mode=%MODE%
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\OE%VER%.json to_user=\"OE%SCHEMAVER%\" log_file=%YADAMU_IMPORT_LOG% mode=%MODE%
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\PM%VER%.json to_user=\"PM%SCHEMAVER%\" log_file=%YADAMU_IMPORT_LOG% mode=%MODE%
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\IX%VER%.json to_user=\"IX%SCHEMAVER%\" log_file=%YADAMU_IMPORT_LOG% mode=%MODE%
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\BI%VER%.json to_user=\"BI%SCHEMAVER%\" log_file=%YADAMU_IMPORT_LOG% mode=%MODE%

