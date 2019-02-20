@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\HR%VER%.json toUser=\"HR%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\SH%VER%.json toUser=\"SH%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\OE%VER%.json toUser=\"OE%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\PM%VER%.json toUser=\"PM%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\IX%VER%.json toUser=\"IX%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\BI%VER%.json toUser=\"BI%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%