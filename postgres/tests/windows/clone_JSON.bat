call env\setEnvironment.bat
@set DIR=JSON\%JSON%
@set MDIR=%TESTDATA%\%JSON&
@set SCHEMA=JTEST
@set FILENAME=testcase
@set SCHVER=1
mkdir %DIR%
psql -U %DB_USER% -h %DB_HOST% -a -f ..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHVER% -vMETHOD='SAX' -f sql\RECREATE_SCHEMA.sql >> %LOGDIR%\RECREATE_SCHEMA.log
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%MDIR%\%FILENAME%.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%DIR%\%FILENAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHVER% -vMETHOD='SAX' -f sql\RECREATE_SCHEMA.sql >> %LOGDIR%\RECREATE_SCHEMA.log
node ..\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%DIR%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHVER%\" logFile=%IMPORTLOG%
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHVER% -vMETHOD=SAX -f sql\COMPARE_SCHEMA.sql >> %LOGDIR%\COMPARE_SCHEMA.log 
node ..\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%DIR%\%FILENAME%%SCHVER%.json owner=\"%SCHEMA%%SCHVER%\" mode=%MODE% logFile=%EXPORTLOG%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false