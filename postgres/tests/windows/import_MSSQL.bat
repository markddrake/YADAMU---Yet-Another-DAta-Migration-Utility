@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Northwind%VER%.json        toUser=\"Northwind%SCHEMAVER%\"      logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Sales%VER%.json            toUser=\"Sales%SCHEMAVER%\"          logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Person%VER%.json           toUser=\"Person%SCHEMAVER%\"         logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Production%VER%.json       toUser=\"Production%SCHEMAVER%\"     logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Purchasing%VER%.json       toUser=\"Purchasing%SCHEMAVER%\"     logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\HumanResources%VER%.json   toUser=\"HumanResources%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"DW%SCHEMAVER%\"             logFile=%IMPORTLOG% mode=%MODE%
