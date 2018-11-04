@set TGT=%~1
@set UID=%~2
@set VER=%~3
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%TGT%\HR%VER%.json --owner=HR%UID%
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%TGT%\SH%VER%.json --owner=SH%UID%
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%TGT%\OE%VER%.json --owner=OE%UID%
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%TGT%\PM%VER%.json --owner=PM%UID%
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%TGT%\IX%VER%.json --owner=IX%UID%
node ..\node\export --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%TGT%\BI%VER%.json --owner=BI%UID%
