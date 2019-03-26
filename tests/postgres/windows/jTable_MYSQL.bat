@set YADAMU_TARGET=MySQL\jTable
@set YADAMU_PARSER=RDBMS
call ..\windows\initialize.bat %~dp0
@set YADAMU_INPUT_PATH=%YADAMU_INPUT_PATH:~0,-7%
psql -U %DB_USER% -h %DB_HOST% -a -f %YADAMU_DB_ROOT%\sql\JSON_IMPORT.sql >> %YADAMU_LOG_PATH%\install\JSON_IMPORT.log
@set FILENAME=sakila
@set SCHEMA=SAKILA
@set SCHEMAVER=1
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_INPUT_PATH%\%FILENAME%.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD='JSON_TABLE' -f %YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
psql -U %DB_USER% -h %DB_HOST% -q -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SCRIPT_ROOT%\sql\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set FILENAME=jsonExample
@set SCHEMA=JTEST
@set SCHEMAVER=1
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_INPUT_PATH%\%FILENAME%.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
@set SCHEMAVER=2
psql -U %DB_USER% -h %DB_HOST% -a -vSCHEMA=%SCHEMA% -vID=%SCHEMAVER% -vMETHOD='JSON_TABLE' -f %YADAMU_SCRIPT_ROOT%\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\jTableImport --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json toUser=\"%SCHEMA%%SCHEMAVER%\" logFile=%IMPORTLOG%
psql -U %DB_USER% -h %DB_HOST% -q -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -f %YADAMU_SCRIPT_ROOT%\sql\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_DB_ROOT%\node\export --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json owner=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% logFile=%EXPORTLOG%
node %YADAMU_HOME%\utilities\node/compareFileSizes %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH%
node %YADAMU_HOME%\utilities\node/compareArrayContent %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH% false