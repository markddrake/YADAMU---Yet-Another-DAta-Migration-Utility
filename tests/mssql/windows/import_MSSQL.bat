@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHEMAVER%         file=%SRC%\Northwind%VER%.json        toUser=dbo            logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Sales%VER%.json            toUser=Sales          logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Person%VER%.json           toUser=Person         logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Production%VER%.json       toUser=Production     logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\Purchasing%VER%.json       toUser=Purchasing     logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    file=%SRC%\HumanResources%VER%.json   toUser=HumanResources logFile=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHEMAVER%  file=%SRC%\AdventureWorksDW%VER%.json toUser=dbo            logFile=%IMPORTLOG% mode=%MODE%