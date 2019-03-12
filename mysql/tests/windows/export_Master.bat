call env\setEnvironment.bat
@set MDIR=%TESTDATA%\JSON
mkdir %MDIR%
@set SCHEMA=jtest
@set FILENAME=jsonExample
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%MDIR%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% logFile=%EXPORTLOG%
@set MDIR=%TESTDATA%\MySQL
mkdir %MDIR%
@set SCHEMA=sakila
@set FILENAME=sakila
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%MDIR%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% logFile=%EXPORTLOG%
