call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHEMAVER=1
mkdir %DIR%
psql -U %DB_USER% -h %DB_HOST% -a -f ..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHEMAVER% -vMETHOD=Clarinet -f sql\RECREATE_ORACLE_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle.bat %MDIR% %SCHEMAVER% ""
call windows\export_Oracle.bat %DIR% %SCHEMAVER% %SCHEMAVER% %MODE%
@set SCHEMAVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHEMAVER% -vMETHOD=Clarinet -f sql\RECREATE_ORACLE_ALL.sql>>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle.bat %DIR% %SCHEMAVER% 1 
psql -U %DB_USER% -h %DB_HOST% -q -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=Clarinet -f sql\COMPARE_ORACLE_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_Oracle.bat %DIR% %SCHEMAVER% %SCHEMAVER% %MODE% 
node ..\..\utilities\node/compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\node/compareArrayContent %LOGDIR% %MDIR% %DIR% false