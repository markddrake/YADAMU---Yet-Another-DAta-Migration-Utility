@set SRC=%~1
@set USCHEMA=%~2
@set UID=%~3
@set VER=%~4
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Northwind%VER%.json        --TOUSER=\"%USCHEMA%%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Sales%VER%.json            --TOUSER=\"%USCHEMA%%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Person%VER%.json           --TOUSER=\"%USCHEMA%%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Production%VER%.json       --TOUSER=\"%USCHEMA%%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\Purchasing%VER%.json       --TOUSER=\"%USCHEMA%%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\HumanResources%VER%.json   --TOUSER=\"%USCHEMA%%UID%\" 
node ..\node\jTableImport  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --FILE=%SRC%\\AdventureWorksDW%VER%.json --TOUSER=\"%USCHEMA%%UID%\" 
