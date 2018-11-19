@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Northwind%VER%.json        toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Sales%VER%.json            toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Person%VER%.json           toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Production%VER%.json       toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\Purchasing%VER%.json       toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\HumanResources%VER%.json   toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%DB_DBNAME% file=%SRC%\AdventureWorksDW%VER%.json toUser=%SCHEMA%%SCHVER% logFile=%IMPORTLOG% mode=%MODE%