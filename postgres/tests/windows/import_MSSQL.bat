@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Northwind%VER%.json        toUser=\"Northwind%SCHVER%\"      logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Sales%VER%.json            toUser=\"Sales%SCHVER%\"          logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Person%VER%.json           toUser=\"Person%SCHVER%\"         logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Production%VER%.json       toUser=\"Production%SCHVER%\"     logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Purchasing%VER%.json       toUser=\"Purchasing%SCHVER%\"     logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\HumanResources%VER%.json   toUser=\"HumanResources%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"DW%SCHVER%\"             logFile=%IMPORTLOG% mode=%MODE%
