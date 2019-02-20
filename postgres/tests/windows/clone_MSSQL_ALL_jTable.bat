call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL%
@set SCHEMAVER=1
mkdir %DIR%
psql -U %DB_USER% -h %DB_HOST% -a -f ..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHEMAVER% -vMETHOD=JSON_TABLE -f sql\RECREATE_MSSQL_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_MSSQL_jTable.bat %MDIR% %SCHEMAVER% ""
call windows\export_MSSQL %DIR% %SCHEMAVER% %SCHEMAVER%
@set SCHEMAVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHEMAVER% -vMETHOD=JSON_TABLE -f sql\RECREATE_MSSQL_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_MSSQL_jTable.bat %DIR% %SCHEMAVER% 1
psql -U %DB_USER% -h %DB_HOST% -a -vID1=1 -vID2=2 -vMETHOD=JSON_TABLE -f sql\COMPARE_MSSQL_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_MSSQL %DIR% %SCHEMAVER% %SCHEMAVER%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false