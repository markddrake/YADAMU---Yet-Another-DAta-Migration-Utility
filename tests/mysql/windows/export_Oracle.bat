@set TGT=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\HR%FILEVER%.json owner=\"HR%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\SH%FILEVER%.json owner=\"SH%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\OE%FILEVER%.json owner=\"OE%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\PM%FILEVER%.json owner=\"PM%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\IX%FILEVER%.json owner=\"IX%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_DB_ROOT%\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\BI%FILEVER%.json owner=\"BI%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
