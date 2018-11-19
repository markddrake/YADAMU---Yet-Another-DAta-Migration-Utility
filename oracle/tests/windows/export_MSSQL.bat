@SET TGT=%~1
@SET VER=%~2
@SET SCHVER=%~3
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Northwind%SCHVER%\"       file=%TGT%\Northwind%VER%.json        logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Sales%SCHVER%\"           file=%TGT%\Sales%VER%.json            logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Person%SCHVER%\"          file=%TGT%\Person%VER%.json           logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Production%SCHVER%\"      file=%TGT%\Production%VER%.json       logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Purchasing%SCHVER%\"      file=%TGT%\Purchasing%VER%.json       logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"HumanResources%SCHVER%\"  file=%TGT%\HumanResources%VER%.json   logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"DW%SCHVER%\"              file=%TGT%\AdventureWorksDW%VER%.json logFile=%EXPORTLOG% mode=%MODE%
