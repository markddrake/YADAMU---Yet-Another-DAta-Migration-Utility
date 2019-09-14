@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD%  --database=%DB_DBNAME% file=%SRC%\HR%FILEVER%.json to_user=\"HR%SCHEMAVER%\" log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD%  --database=%DB_DBNAME% file=%SRC%\SH%FILEVER%.json to_user=\"SH%SCHEMAVER%\" log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD%  --database=%DB_DBNAME% file=%SRC%\OE%FILEVER%.json to_user=\"OE%SCHEMAVER%\" log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD%  --database=%DB_DBNAME% file=%SRC%\PM%FILEVER%.json to_user=\"PM%SCHEMAVER%\" log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD%  --database=%DB_DBNAME% file=%SRC%\IX%FILEVER%.json to_user=\"IX%SCHEMAVER%\" log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import  --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD%  --database=%DB_DBNAME% file=%SRC%\BI%FILEVER%.json to_user=\"BI%SCHEMAVER%\" log_file=%IMPORTLOG%

