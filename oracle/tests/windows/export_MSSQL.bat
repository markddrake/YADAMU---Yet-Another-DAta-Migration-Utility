@SET TGT=%~1
@SET VER=%~2
@SET SCHEMAVER=%~3
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Northwind%SCHEMAVER%\"       file=%TGT%\Northwind%VER%.json        logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Sales%SCHEMAVER%\"           file=%TGT%\Sales%VER%.json            logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Person%SCHEMAVER%\"          file=%TGT%\Person%VER%.json           logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Production%SCHEMAVER%\"      file=%TGT%\Production%VER%.json       logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Purchasing%SCHEMAVER%\"      file=%TGT%\Purchasing%VER%.json       logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"HumanResources%SCHEMAVER%\"  file=%TGT%\HumanResources%VER%.json   logFile=%EXPORTLOG% mode=%MODE%
node ..\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"DW%SCHEMAVER%\"              file=%TGT%\AdventureWorksDW%VER%.json logFile=%EXPORTLOG% mode=%MODE%
