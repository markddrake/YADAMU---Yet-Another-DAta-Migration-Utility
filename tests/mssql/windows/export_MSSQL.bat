@SET TGT=%~1
@SET VER=%~2
@SET SCHEMAVER=%~3
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=Northwind%SCHEMAVER%         owner=dbo               file=%TGT%\Northwind%VER%.json        mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    owner=Sales             file=%TGT%\Sales%VER%.json            mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    owner=Person            file=%TGT%\Person%VER%.json           mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    owner=Production        file=%TGT%\Production%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    owner=Purchasing        file=%TGT%\Purchasing%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorks%SCHEMAVER%    owner=HumanResources    file=%TGT%\HumanResources%VER%.json   mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --HOSTNAME=%DB_HOST% --password=%DB_PWD% --database=AdventureWorksDW%SCHEMAVER%  owner=dbo               file=%TGT%\AdventureWorksDW%VER%.json mode=%MODE% logFile=%EXPORTLOG%
