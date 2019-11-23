@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_BIN%\upload rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HR%FILEVER%.json to_user=\"HR%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\upload rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\SH%FILEVER%.json to_user=\"SH%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\upload rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\OE%FILEVER%.json to_user=\"OE%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\upload rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\PM%FILEVER%.json to_user=\"PM%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\upload rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\IX%FILEVER%.json to_user=\"IX%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\upload rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\BI%FILEVER%.json to_user=\"BI%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%