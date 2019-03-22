@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\HR%FILEVER%.json toUser=\"HR%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\SH%FILEVER%.json toUser=\"SH%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\OE%FILEVER%.json toUser=\"OE%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\PM%FILEVER%.json toUser=\"PM%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\IX%FILEVER%.json toUser=\"IX%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%SRC%\BI%FILEVER%.json toUser=\"BI%SCHEMAVER%\" logFile=%IMPORTLOG%

