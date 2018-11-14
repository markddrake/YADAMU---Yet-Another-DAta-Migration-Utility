@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Northwind%VER%.json        toUser=\"Northwind%SCHVER%\"      logFile=%IMPORTLOG%  
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Sales%VER%.json            toUser=\"Sales%SCHVER%\"          logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Person%VER%.json           toUser=\"Person%SCHVER%\"         logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Production%VER%.json       toUser=\"Production%SCHVER%\"     logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Purchasing%VER%.json       toUser=\"Purchasing%SCHVER%\"     logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HumanResources%VER%.json   toUser=\"HumanResources%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"DW%SCHVER%\"             logFile=%IMPORTLOG%
