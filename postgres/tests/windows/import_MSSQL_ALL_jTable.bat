@set SRC=%~1
@set USCHEMA=%~2
@set SCHEMAVER=%~3
@set VER=%~4
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Northwind%VER%.json        toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE% 
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Sales%VER%.json            toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Person%VER%.json           toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Production%VER%.json       toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\Purchasing%VER%.json       toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\HumanResources%VER%.json   toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"%USCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
