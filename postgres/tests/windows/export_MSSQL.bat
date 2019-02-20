@SET TGT=%~1
@SET VER=%~2
@SET SCHEMAVER=%~3
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"Northwind%SCHEMAVER%\"      file=%TGT%\Northwind%VER%.json        mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"Sales%SCHEMAVER%\"          file=%TGT%\Sales%VER%.json            mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"Person%SCHEMAVER%\"         file=%TGT%\Person%VER%.json           mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"Production%SCHEMAVER%\"     file=%TGT%\Production%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"Purchasing%SCHEMAVER%\"     file=%TGT%\Purchasing%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"HumanResources%SCHEMAVER%\" file=%TGT%\HumanResources%VER%.json   mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% owner=\"DW%SCHEMAVER%\"             file=%TGT%\AdventureWorksDW%VER%.json mode=%MODE% logFile=%EXPORTLOG%
