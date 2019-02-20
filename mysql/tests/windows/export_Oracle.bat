@set TGT=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\HR%VER%.json owner=\"HR%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\SH%VER%.json owner=\"SH%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\OE%VER%.json owner=\"OE%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\PM%VER%.json owner=\"PM%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\IX%VER%.json owner=\"IX%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\BI%VER%.json owner=\"BI%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
