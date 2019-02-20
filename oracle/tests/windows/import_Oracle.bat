@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HR%VER%.json toUser=\"HR%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\SH%VER%.json toUser=\"SH%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\OE%VER%.json toUser=\"OE%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\PM%VER%.json toUser=\"PM%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\IX%VER%.json toUser=\"IX%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\BI%VER%.json toUser=\"BI%SCHEMAVER%\" logFile=%IMPORTLOG% mode=%MODE%

