@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Northwind%FILEVER%.json        toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Sales%FILEVER%.json            toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Person%FILEVER%.json           toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Production%FILEVER%.json       toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Purchasing%FILEVER%.json       toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\HumanResources%FILEVER%.json   toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\AdventureWorksDW%FILEVER%.json toUser=%SCHEMA%%SCHEMAVER% logFile=%IMPORTLOG% mode=%MODE%