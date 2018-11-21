call env\setEnvironment.bat
@set DIR=JSON\%MSSQL%
@set MDIR=%TESTDATA%\%MSSQL%
@set SCHVER=1
mkdir %DIR%
psql -U %DB_USER% -h %DB_HOST% -a -f ..\sql\JSON_IMPORT.sql >> %LOGDIR%\install\JSON_IMPORT.log
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHVER% -vMETHOD='SAX' -f sql\RECREATE_MSSQL_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_MSSQL.bat %MDIR% %SCHVER% ""
call windows\export_MSSQL %DIR% %SCHVER% %SCHVER%
@set SCHVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vID=%SCHVER% -vMETHOD='SAX' -f sql\RECREATE_MSSQL_ALL.sql >>%LOGDIR%\RECREATE_SCHEMA.log
call windows\import_MSSQL.bat %DIR% %SCHVER% 1
psql -U %DB_USER% -h %DB_HOST% -a -vID1=1 -vID2=2-vMETHOD='SAX' -f sql\COMPARE_MSSQL_ALL.sql >>%LOGDIR%\COMPARE_SCHEMA.log
call windows\export_MSSQL %DIR% %SCHVER% %SCHVER%
node ..\..\utilities\compareFileSizes %LOGDIR% %MDIR% %DIR%
node ..\..\utilities\compareArrayContent %LOGDIR% %MDIR% %DIR% false