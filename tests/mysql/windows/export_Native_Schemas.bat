if not defined MODE set MODE=DATA_ONLY
@set YADAMU_TARGET=MySQL
call ..\windows\initialize.bat %~dp0
if not exist %YADAMU_INPUT_PATH%\ mkdir %YADAMU_INPUT_PATH%
@set FILENAME=jsonExample
@set SCHEMA=jtest
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%YADAMU_INPUT_PATH%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% log_file=%EXPORTLOG%
@set FILENAME=sakila
@set SCHEMA=sakila
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%YADAMU_INPUT_PATH%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% log_file=%EXPORTLOG%