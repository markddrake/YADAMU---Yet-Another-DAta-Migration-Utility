@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Northwind%VER%.json        toUser=\"Northwind%SCHEMAVER%\"        logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Sales%VER%.json            toUser=\"Sales%SCHEMAVER%\"            logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Person%VER%.json           toUser=\"Person%SCHEMAVER%\"           logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Production%VER%.json       toUser=\"Production%SCHEMAVER%\"       logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Purchasing%VER%.json       toUser=\"Purchasing%SCHEMAVER%\"       logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HumanResources%VER%.json   toUser=\"HumanResources%SCHEMAVER%\"   logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"DW%SCHEMAVER%\"               logFile=%IMPORTLOG% mode=%MODE%
