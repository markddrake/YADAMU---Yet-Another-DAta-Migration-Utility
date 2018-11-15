@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\jTableImport  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\HR%VER%.json toUser=\"HR%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\jTableImport  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\SH%VER%.json toUser=\"SH%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\jTableImport  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\OE%VER%.json toUser=\"OE%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\jTableImport  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\PM%VER%.json toUser=\"PM%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\jTableImport  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\IX%VER%.json toUser=\"IX%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\jTableImport  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\BI%VER%.json toUser=\"BI%SCHVER%\" logFile=%IMPORTLOG%