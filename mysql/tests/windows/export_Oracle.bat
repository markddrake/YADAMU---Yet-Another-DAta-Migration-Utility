@set TGT=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\HR%VER%.json owner=\"HR%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\SH%VER%.json owner=\"SH%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\OE%VER%.json owner=\"OE%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\PM%VER%.json owner=\"PM%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\IX%VER%.json owner=\"IX%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export  --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD%  --port=%DB_PORT% --database=%DB_DBNAME% file=%TGT%\BI%VER%.json owner=\"BI%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
