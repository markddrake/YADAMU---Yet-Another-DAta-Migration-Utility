@set SRC=%~1
@set UID=%~2
@set VER=%~3
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%SRC%\\HR%VER%.json --toUser=HR%UID%
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%SRC%\\SH%VER%.json --toUser=SH%UID%
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%SRC%\\OE%VER%.json --toUser=OE%UID%
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%SRC%\\PM%VER%.json --toUser=PM%UID%
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%SRC%\\IX%VER%.json --toUser=IX%UID%
node ..\node\jTableImport --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% --File=%SRC%\\BI%VER%.json --toUser=BI%UID%

