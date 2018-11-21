call env\setEnvironment.bat
@set DIR=JSON\%ORCL%
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
@set SCHVER=1
mkdir %DIR%
psql -U %DB_USER% -h %DB_HOST% -a -f ..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHVER% -vMETHOD=JSON_TABLE -f sql\RECREATE_ORACLE_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle_jTable.bat %MDIR% %SCHVER% ""
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE%
@set SCHVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHVER% -vMETHOD=JSON_TABLE -f sql\RECREATE_ORACLE_ALL.sql>>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_Oracle_jTable.bat %DIR% %SCHVER% 1 
psql -U %DB_USER% -h %DB_HOST% -a -vID1=1 -vID2=%SCHVER% -vMETHOD=JSON_TABLE -f sql\COMPARE_ORACLE_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_Oracle.bat %DIR% %SCHVER% %SCHVER% %MODE% 
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false