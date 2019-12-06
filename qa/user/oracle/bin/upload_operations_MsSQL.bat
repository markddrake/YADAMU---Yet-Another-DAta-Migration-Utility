@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Northwind%FILEVER%.json        to_user=\"Northwind%SCHEMAVER%\"        mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Sales%FILEVER%.json            to_user=\"Sales%SCHEMAVER%\"            mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Person%FILEVER%.json           to_user=\"Person%SCHEMAVER%\"           mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Production%FILEVER%.json       to_user=\"Production%SCHEMAVER%\"       mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\Purchasing%FILEVER%.json       to_user=\"Purchasing%SCHEMAVER%\"       mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\HumanResources%FILEVER%.json   to_user=\"HumanResources%SCHEMAVER%\"   mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
node %YADAMU_BIN%\upload  rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%SRC%\AdventureWorksDW%FILEVER%.json to_user=\"AdventureWorksDW%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG% 
