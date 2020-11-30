@SET TGT=%~1
@SET FILEVER=%~2
@SET SCHEMAVER=%~3
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHEMAVER%         overwrite=yes from_user=dbo            file=%TGT%\Northwind%FILEVER%.json        mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    overwrite=yes from_user=Sales          file=%TGT%\Sales%FILEVER%.json            mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    overwrite=yes from_user=Person         file=%TGT%\Person%FILEVER%.json           mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    overwrite=yes from_user=Production     file=%TGT%\Production%FILEVER%.json       mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    overwrite=yes from_user=Purchasing     file=%TGT%\Purchasing%FILEVER%.json       mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    overwrite=yes from_user=HumanResources file=%TGT%\HumanResources%FILEVER%.json   mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHEMAVER%  overwrite=yes from_user=dbo            file=%TGT%\AdventureWorksDW%FILEVER%.json mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
