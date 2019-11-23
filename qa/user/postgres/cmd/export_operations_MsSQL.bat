@SET TGT=%~1
@SET FILEVER=%~2
@SET SCHEMAVER=%~3
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"Northwind%SCHEMAVER%\"        file=%TGT%\Northwind%FILEVER%.json        mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"Sales%SCHEMAVER%\"            file=%TGT%\Sales%FILEVER%.json            mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"Person%SCHEMAVER%\"           file=%TGT%\Person%FILEVER%.json           mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"Production%SCHEMAVER%\"       file=%TGT%\Production%FILEVER%.json       mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"Purchasing%SCHEMAVER%\"       file=%TGT%\Purchasing%FILEVER%.json       mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"HumanResources%SCHEMAVER%\"   file=%TGT%\HumanResources%FILEVER%.json   mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% overwrite=yes from_user=\"AdventureWorksDW%SCHEMAVER%\" file=%TGT%\AdventureWorksDW%FILEVER%.json mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
