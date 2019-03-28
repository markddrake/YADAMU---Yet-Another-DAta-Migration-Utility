call ..\windows\setEnvironment.bat
@set YADAMU_JSON_ROOT=%TESTDATA%\JSON
mkdir %YADAMU_JSON_ROOT%
@set SCHEMA=jtest
@set FILENAME=jsonExample
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --pass --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%YADAMU_JSON_ROOT%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% logFile=%EXPORTLOG%
@set YADAMU_JSON_ROOT=%TESTDATA%\MySQL
mkdir %YADAMU_JSON_ROOT%
@set SCHEMA=sakila
@set FILENAME=sakila
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --pass --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%YADAMU_JSON_ROOT%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% logFile=%EXPORTLOG%
