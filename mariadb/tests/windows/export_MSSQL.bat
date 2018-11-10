@SET TGT=%~1
@SET VER=%~2
@SET ID=%~3
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"Northwind%UID%\"       --FILE=%TGT%\Northwind%VER%.json
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"Sales%UID%\"           --FILE=%TGT%\Sales%VER%.json
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"Person%UID%\"          --FILE=%TGT%\Person%VER%.json
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"Production%UID%\"      --FILE=%TGT%\Production%VER%.json
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"Purchasing%UID%\"      --FILE=%TGT%\Purchasing%VER%.json
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"HumanResources%UID%\"  --FILE=%TGT%\HumanResources%VER%.json
node ..\node\export  --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --OWNER=\"DW%UID%\"              --FILE=%TGT%\AdventureWorksDW%VER%.json
