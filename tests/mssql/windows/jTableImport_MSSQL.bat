@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHEMAVER%         file=%SRC%\Northwind%FILEVER%.json        toUser=dbo            logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Sales%FILEVER%.json            toUser=Sales          logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Person%FILEVER%.json           toUser=Person         logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Production%FILEVER%.json       toUser=Production     logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Purchasing%FILEVER%.json       toUser=Purchasing     logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\HumanResources%FILEVER%.json   toUser=HumanResources logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHEMAVER%  file=%SRC%\AdventureWorksDW%FILEVER%.json toUser=dbo            logFile=%IMPORTLOG% mode=%MODE%