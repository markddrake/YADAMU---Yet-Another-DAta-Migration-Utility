@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Northwind%FILEVER%.json        toUser=\"Northwind%SCHEMAVER%\"       mode=%MODE% logFile=%IMPORTLOG% 
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Sales%FILEVER%.json            toUser=\"Sales%SCHEMAVER%\"           mode=%MODE% logFile=%IMPORTLOG% 
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Person%FILEVER%.json           toUser=\"Person%SCHEMAVER%\"          mode=%MODE% logFile=%IMPORTLOG% 
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Production%FILEVER%.json       toUser=\"Production%SCHEMAVER%\"      mode=%MODE% logFile=%IMPORTLOG% 
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Purchasing%FILEVER%.json       toUser=\"Purchasing%SCHEMAVER%\"      mode=%MODE% logFile=%IMPORTLOG% 
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HumanResources%FILEVER%.json   toUser=\"HumanResources%SCHEMAVER%\"  mode=%MODE% logFile=%IMPORTLOG% 
node %YADAMU_DB_ROOT%\node\jTableImport  userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\AdventureWorksDW%FILEVER%.json toUser=\"DW%SCHEMAVER%\"              mode=%MODE% logFile=%IMPORTLOG% 
