@set TGT=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\HR%VER%.json owner=\"HR%SCHVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\SH%VER%.json owner=\"SH%SCHVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\OE%VER%.json owner=\"OE%SCHVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\PM%VER%.json owner=\"PM%SCHVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\IX%VER%.json owner=\"IX%SCHVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\BI%VER%.json owner=\"BI%SCHVER%\" logFile=%EXPORTLOG% mode=%MODE% 
