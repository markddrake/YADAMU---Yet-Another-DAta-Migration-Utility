@SET TGT=%~1
@SET VER=%~2
@SET SCHEMAVER=%~3
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Northwind%SCHEMAVER%\"       file=%TGT%\Northwind%VER%.json        mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Sales%SCHEMAVER%\"           file=%TGT%\Sales%VER%.json            mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Person%SCHEMAVER%\"          file=%TGT%\Person%VER%.json           mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Production%SCHEMAVER%\"      file=%TGT%\Production%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"Purchasing%SCHEMAVER%\"      file=%TGT%\Purchasing%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"HumanResources%SCHEMAVER%\"  file=%TGT%\HumanResources%VER%.json   mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% owner=\"DW%SCHEMAVER%\"              file=%TGT%\AdventureWorksDW%VER%.json mode=%MODE% logFile=%EXPORTLOG%
