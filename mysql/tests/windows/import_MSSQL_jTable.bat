@set SRC=%~1
@set UID=%~2
@set VER=%~3
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Northwind%VER%.json        --TOUSER=\"Northwind%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Sales%VER%.json            --TOUSER=\"Sales%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Person%VER%.json           --TOUSER=\"Person%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Production%VER%.json       --TOUSER=\"Production%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Purchasing%VER%.json       --TOUSER=\"Purchasing%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\HumanResources%VER%.json   --TOUSER=\"HumanResources%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\AdventureWorksDW%VER%.json --TOUSER=\"DW%UID%\" 
