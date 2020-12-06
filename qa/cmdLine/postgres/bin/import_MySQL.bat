@echo %YADAMU_TRACE%
call qa\cmdLine\bin\initialize.bat %~dp0 %~dp0 mysql import %YADAMU_TESTNAME%
set YADAMU_PARSER=CLARINET
set SCHEMA=sakila
set SCHEMA_VERSION=1
set FILENAME=sakila
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMA_VERSION% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_BIN%\import.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_IMPORT_MYSQL%\%FILENAME%.json TO_USER=\"%SCHEMA%%SCHEMA_VERSION%\" LOG_FILE=%YADAMU_IMPORT_LOG% EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMA_VERSION%.json  FROM_USER=\"%SCHEMA%%SCHEMA_VERSION%\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
set PRIOR_VERSION=%SCHEMA_VERSION%
set /a SCHEMA_VERSION+=1
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMA_VERSION% -vMETHOD='JSON_TABLE' -f %YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_BIN%\import.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_OUTPUT_PATH%\%FILENAME%%PRIOR_VERSION%.json TO_USER=\"%SCHEMA%%SCHEMA_VERSION%\" LOG_FILE=%YADAMU_IMPORT_LOG% EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -q -vSCHEMA=%SCHEMA% -vID1=%PRIOR_VERSION% -vID2=%SCHEMA_VERSION% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMA_VERSION%.json  FROM_USER=\"%SCHEMA%%SCHEMA_VERSION%\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
set SCHEMA=jtest
set SCHEMA_VERSION=1
set FILENAME=jsonExample
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMA_VERSION% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_BIN%\import.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_IMPORT_MYSQL%\%FILENAME%.json TO_USER=\"%SCHEMA%%SCHEMA_VERSION%\" LOG_FILE=%YADAMU_IMPORT_LOG% EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMA_VERSION%.json  FROM_USER=\"%SCHEMA%%SCHEMA_VERSION%\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
set PRIOR_VERSION=%SCHEMA_VERSION%
set /a SCHEMA_VERSION+=1
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMA_VERSION% -vMETHOD='JSON_TABLE' -f %YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_BIN%\import.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_OUTPUT_PATH%\%FILENAME%%PRIOR_VERSION%.json TO_USER=\"%SCHEMA%%SCHEMA_VERSION%\" LOG_FILE=%YADAMU_IMPORT_LOG% EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -q -vSCHEMA=%SCHEMA% -vID1=%PRIOR_VERSION% -vID2=%SCHEMA_VERSION% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
call %YADAMU_BIN%\export.bat --RDBMS=%YADAMU_VENDOR% --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD% --DATABASE=%DB_DBNAME%  FILE=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMA_VERSION%.json  FROM_USER=\"%SCHEMA%%SCHEMA_VERSION%\"  MODE=%MODE% LOG_FILE=%YADAMU_EXPORT_LOG%  EXCEPTION_FOLDER=%YADAMU_LOG_PATH%
node %YADAMU_QA_JSPATH%\compareFileSizes %YADAMU_LOG_PATH% %YADAMU_IMPORT_MYSQL% %YADAMU_OUTPUT_PATH%
node %YADAMU_QA_JSPATH%\compareArrayContent %YADAMU_LOG_PATH% %YADAMU_IMPORT_MYSQL% %YADAMU_OUTPUT_PATH% false