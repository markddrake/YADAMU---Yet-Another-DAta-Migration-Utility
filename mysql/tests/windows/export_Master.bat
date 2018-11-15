call env\setEnvironment.bat
@set MDIR=%TESTDATA%\MySQL
mkdir %MDIR%
@set SCHEMA=sakila
@set FILENAME=sakila
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% --file=%MDIR%\%FILENAME%.json owner=\"%SCHEMA%\" mode=%MODE% logFile=%EXPORTLOG%
