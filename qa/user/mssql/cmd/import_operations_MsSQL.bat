@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHEMAVER%        file=%SRC%\Northwind%VER%.json        to_user=dbo            mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%   file=%SRC%\Sales%VER%.json            to_user=Sales          mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%   file=%SRC%\Person%VER%.json           to_user=Person         mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%   file=%SRC%\Production%VER%.json       to_user=Production     mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%   file=%SRC%\Purchasing%VER%.json       to_user=Purchasing     mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%   file=%SRC%\HumanResources%VER%.json   to_user=HumanResources mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHEMAVER% file=%SRC%\AdventureWorksDW%VER%.json to_user=dbo            mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 