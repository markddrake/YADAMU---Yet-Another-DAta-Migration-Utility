@set TGT=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\HR%VER%.json owner=\"HR%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\SH%VER%.json owner=\"SH%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\OE%VER%.json owner=\"OE%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\PM%VER%.json owner=\"PM%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\IX%VER%.json owner=\"IX%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\BI%VER%.json owner=\"BI%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
