@set SRC=%~1
@set USCHEMA=%~2
@set SCHVER=%~3
@set VER=%~4
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Northwind%VER%.json        toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Sales%VER%.json            toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Person%VER%.json           toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Production%VER%.json       toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Purchasing%VER%.json       toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HumanResources%VER%.json   toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\import  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"%USCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
