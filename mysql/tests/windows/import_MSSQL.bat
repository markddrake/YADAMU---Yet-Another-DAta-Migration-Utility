@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Northwind%VER%.json        toUser=\"Northwind%SCHVER%\"      logFile=%IMPORTLOG%  
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Sales%VER%.json            toUser=\"Sales%SCHVER%\"          logFile=%IMPORTLOG%
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Person%VER%.json           toUser=\"Person%SCHVER%\"         logFile=%IMPORTLOG%
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Production%VER%.json       toUser=\"Production%SCHVER%\"     logFile=%IMPORTLOG%
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Purchasing%VER%.json       toUser=\"Purchasing%SCHVER%\"     logFile=%IMPORTLOG%
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\HumanResources%VER%.json   toUser=\"HumanResources%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"DW%SCHVER%\"             logFile=%IMPORTLOG%