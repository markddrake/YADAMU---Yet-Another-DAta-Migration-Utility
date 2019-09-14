@set SRC=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node %YADAMU_DB_ROOT%\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HR%VER%.json to_user=\"HR%SCHEMAVER%\" mode=%MODE% log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\SH%VER%.json to_user=\"SH%SCHEMAVER%\" mode=%MODE% log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\OE%VER%.json to_user=\"OE%SCHEMAVER%\" mode=%MODE% log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\PM%VER%.json to_user=\"PM%SCHEMAVER%\" mode=%MODE% log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\IX%VER%.json to_user=\"IX%SCHEMAVER%\" mode=%MODE% log_file=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\import userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\BI%VER%.json to_user=\"BI%SCHEMAVER%\" mode=%MODE% log_file=%IMPORTLOG%

