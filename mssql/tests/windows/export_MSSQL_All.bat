@SET TGT=%~1
@SET VER=%~2
@SET SCHVER=%~3
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHVER%         owner=dbo               file=%TGT%\Northwind%VER%.json        mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    owner=Sales             file=%TGT%\Sales%VER%.json            mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    owner=Person            file=%TGT%\Person%VER%.json           mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    owner=Production        file=%TGT%\Production%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    owner=Purchasing        file=%TGT%\Purchasing%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHVER%    owner=HumanResources    file=%TGT%\HumanResources%VER%.json   mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHVER%  owner=dbo               file=%TGT%\AdventureWorksDW%VER%.json mode=%MODE% logFile=%EXPORTLOG%
