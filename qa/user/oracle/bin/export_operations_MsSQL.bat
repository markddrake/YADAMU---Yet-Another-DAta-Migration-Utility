@SET TGT=%~1
@SET VER=%~2
@SET SCHEMAVER=%~3
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"Northwind%SCHEMAVER%\"        file=%TGT%\Northwind%VER%.json        mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"Sales%SCHEMAVER%\"            file=%TGT%\Sales%VER%.json            mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"Person%SCHEMAVER%\"           file=%TGT%\Person%VER%.json           mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"Production%SCHEMAVER%\"       file=%TGT%\Production%VER%.json       mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"Purchasing%SCHEMAVER%\"       file=%TGT%\Purchasing%VER%.json       mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"HumanResources%SCHEMAVER%\"   file=%TGT%\HumanResources%VER%.json   mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% overwrite=yes from_user=\"AdventureWorksDW%SCHEMAVER%\" file=%TGT%\AdventureWorksDW%VER%.json mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
