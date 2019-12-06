@set TGT=%~1
@set SCHEMAVER=%~2
@set VER=%~3
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\HR%VER%.json overwrite=yes from_user=\"HR%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\SH%VER%.json overwrite=yes from_user=\"SH%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\OE%VER%.json overwrite=yes from_user=\"OE%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\PM%VER%.json overwrite=yes from_user=\"PM%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\IX%VER%.json overwrite=yes from_user=\"IX%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_BIN%\export rdbms=%YADAMU_DB% userid=%DB_USER%/%DB_PWD%@%DB_CONNECTION% file=%TGT%\BI%VER%.json overwrite=yes from_user=\"BI%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_EXPORT_LOG%
