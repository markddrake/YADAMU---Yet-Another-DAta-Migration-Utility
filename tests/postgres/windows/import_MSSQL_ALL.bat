@set SRC=%~1
@set USCHEMA=%~2
@set SCHEMAVER=%~3
@set FILEVER=%~4
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Northwind%FILEVER%.json        toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Sales%FILEVER%.json            toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Person%FILEVER%.json           toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Production%FILEVER%.json       toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Purchasing%FILEVER%.json       toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\HumanResources%FILEVER%.json   toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\AdventureWorksDW%FILEVER%.json toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
