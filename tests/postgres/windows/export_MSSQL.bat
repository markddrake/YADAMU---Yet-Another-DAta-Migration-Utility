@SET TGT=%~1
@SET FILEVER=%~2
@SET SCHEMAVER=%~3
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"Northwind%SCHEMAVER%\"      file=%TGT%\Northwind%FILEVER%.json        mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"Sales%SCHEMAVER%\"          file=%TGT%\Sales%FILEVER%.json            mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"Person%SCHEMAVER%\"         file=%TGT%\Person%FILEVER%.json           mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"Production%SCHEMAVER%\"     file=%TGT%\Production%FILEVER%.json       mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"Purchasing%SCHEMAVER%\"     file=%TGT%\Purchasing%FILEVER%.json       mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"HumanResources%SCHEMAVER%\" file=%TGT%\HumanResources%FILEVER%.json   mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% owner=\"DW%SCHEMAVER%\"             file=%TGT%\AdventureWorksDW%FILEVER%.json mode=%MODE% logFile=%EXPORTLOG%
