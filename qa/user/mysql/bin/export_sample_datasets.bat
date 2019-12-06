call qa\bin\initialize.bat %1 %~dp0 mysql export
@set YADAMU_OUTPUT_BASE=%YADAMU_HOME%\JSON\
if not exist %YADAMU_OUTPUT_BASE%\ mkdir %YADAMU_OUTPUT_BASE%
@set YADAMU_OUTPUT_PATH=%YADAMU_OUTPUT_BASE%\%YADAMU_TARGET%
if not exist %YADAMU_OUTPUT_PATH%\ mkdir %YADAMU_OUTPUT_PATH%
if not exist %YADAMU_OUTPUT_PATH%\ mkdir %YADAMU_OUTPUT_PATH%
set MODE=DATA_ONLY
@set SCHEMA=jtest
@set FILENAME=jsonExample
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%YADAMU_INPUT_PATH%\%FILENAME%.json overwrite=yes from_user=\"%SCHEMA%\" mode=%MODE% log_file=%EXPORTLOG%
@set SCHEMA=sakila
@set FILENAME=sakila
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%YADAMU_INPUT_PATH%\%FILENAME%.json overwrite=yes from_user=\"%SCHEMA%\" mode=%MODE% log_file=%EXPORTLOG%