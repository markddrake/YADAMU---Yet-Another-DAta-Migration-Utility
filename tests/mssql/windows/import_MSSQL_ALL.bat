@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\Northwind%FILEVER%.json        toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\Sales%FILEVER%.json            toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\Person%FILEVER%.json           toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\Production%FILEVER%.json       toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\Purchasing%FILEVER%.json       toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\HumanResources%FILEVER%.json   toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%SRC%\AdventureWorksDW%FILEVER%.json toUser=\"dbo\" mode=%MODE% logFile=%IMPORTLOG%