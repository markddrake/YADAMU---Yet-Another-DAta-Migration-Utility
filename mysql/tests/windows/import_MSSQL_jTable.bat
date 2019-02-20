@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Northwind%VER%.json        toUser=\"Northwind%SCHEMAVER%\" 
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Sales%VER%.json            toUser=\"Sales%SCHEMAVER%\" 
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Person%VER%.json           toUser=\"Person%SCHEMAVER%\" 
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Production%VER%.json       toUser=\"Production%SCHEMAVER%\" 
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\Purchasing%VER%.json       toUser=\"Purchasing%SCHEMAVER%\" 
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\HumanResources%VER%.json   toUser=\"HumanResources%SCHEMAVER%\" 
node ..\node\jTableImport   --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\AdventureWorksDW%VER%.json toUser=\"DW%SCHEMAVER%\" 
