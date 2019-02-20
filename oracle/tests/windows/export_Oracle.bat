@set TGT=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\HR%VER%.json owner=\"HR%SCHEMAVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\SH%VER%.json owner=\"SH%SCHEMAVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\OE%VER%.json owner=\"OE%SCHEMAVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\PM%VER%.json owner=\"PM%SCHEMAVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\IX%VER%.json owner=\"IX%SCHEMAVER%\" logFile=%EXPORTLOG% mode=%MODE% 
node ..\node\export userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\BI%VER%.json owner=\"BI%SCHEMAVER%\" logFile=%EXPORTLOG% mode=%MODE% 
