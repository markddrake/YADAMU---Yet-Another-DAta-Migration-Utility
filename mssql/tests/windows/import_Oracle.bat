@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\HR%VER%.json toUser=\"HR%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\SH%VER%.json toUser=\"SH%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\OE%VER%.json toUser=\"OE%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\PM%VER%.json toUser=\"PM%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\IX%VER%.json toUser=\"IX%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\BI%VER%.json toUser=\"BI%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%

