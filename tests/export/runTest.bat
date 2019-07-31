REM Run from YADAMU_HOME
@set YADAMU_HOME=%CD%
@set YADAMU_TEST_HOME=%YADAMU_HOME%\tests
@set YADAMU_WORK_ID=export
@set YADAMU_WORK_ROOT=%YADAMU_HOME%\work\%YADAMU_WORK_ID%
if not exist %YADAMU_WORK_ROOT%\ mkdir %YADAMU_WORK_ROOT%
@set YADAMU_LOG_ROOT=%YADAMU_WORK_ROOT%\logs
call %YADAMU_TEST_HOME%\windows\initializeLogging.bat
call %YADAMU_TEST_HOME%\windows\installYadamu.bat
call %YADAMU_TEST_HOME%\%YADAMU_WORK_ID%\createUsers.bat 1
call %YADAMU_TEST_HOME%\windows\createOutputFolders.bat %YADAMU_HOME%
call %YADAMU_HOME%\tests\mssql\env\dbConnection.bat
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=AdventureWorksAll -i%YADAMU_HOME%\tests\mssql\sql\RECREATE_SCHEMA.sql >> %YADAMU_LOG_PATH%\MSSQL_RECREATE_SCHEMA.log
node %YADAMU_TEST_HOME%\node\testHarness CONFIG=%YADAMU_TEST_HOME%\%YADAMU_WORK_ID%\config.json >%YADAMU_LOG_PATH%\%YADAMU_WORK_ID%.log

