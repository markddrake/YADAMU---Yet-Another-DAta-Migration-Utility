call env\setEnvironment.bat
@set DIR=JSON\%JSON%
@set MDIR=%TESTDATA%\%JSON&
@set SCHEMA=JTEST
@set FILENAME=jsonExample
@set SCHEMAVER=1
mkdir %DIR%
psql -U %DB_USER% -h %DB_HOST% -a -f ..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD=Clarinet -f sql\RECREATE_SCHEMA.sql >> %LOGDIR%\RECREATE_SCHEMA.log
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%MDIR%\%FILENAME%.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%DIR%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD=Clarinet -f sql\RECREATE_SCHEMA.sql >> %LOGDIR%\RECREATE_SCHEMA.log
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%DIR%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
psql -U %DB_USER% -h %DB_HOST% -q -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=Clarinet -f sql\COMPARE_SCHEMA.sql >> %LOGDIR%\COMPARE_SCHEMA.log 
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%DIR%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false