@set TGT=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%TGT%\HR%FILEVER%.json overwrite=yes from_user=\"HR%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%TGT%\SH%FILEVER%.json overwrite=yes from_user=\"SH%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%TGT%\OE%FILEVER%.json overwrite=yes from_user=\"OE%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%TGT%\PM%FILEVER%.json overwrite=yes from_user=\"PM%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%TGT%\IX%FILEVER%.json overwrite=yes from_user=\"IX%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%TGT%\BI%FILEVER%.json overwrite=yes from_user=\"BI%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
