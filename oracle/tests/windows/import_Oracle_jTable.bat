@set SRC=%~1
@set SCHVER=%~2
@set VER=%~3
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HR%VER%.json toUser=\"HR%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\SH%VER%.json toUser=\"SH%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\OE%VER%.json toUser=\"OE%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\PM%VER%.json toUser=\"PM%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\IX%VER%.json toUser=\"IX%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%
node ..\node\jTableImport userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\BI%VER%.json toUser=\"BI%SCHVER%\" logFile=%IMPORTLOG% mode=%MODE%