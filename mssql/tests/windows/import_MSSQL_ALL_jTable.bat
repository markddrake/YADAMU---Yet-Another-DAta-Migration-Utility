@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHVER%         file=%SRC%\Northwind%VER%.json        toUser=dbo            logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    file=%SRC%\Sales%VER%.json            toUser=Sales          logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    file=%SRC%\Person%VER%.json           toUser=Person         logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    file=%SRC%\Production%VER%.json       toUser=Production     logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    file=%SRC%\Purchasing%VER%.json       toUser=Purchasing     logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    file=%SRC%\HumanResources%VER%.json   toUser=HumanResources logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHVER%  file=%SRC%\AdventureWorksDW%VER%.json toUser=dbo            logFile=%IMPORTLOG% mode=%MODE%