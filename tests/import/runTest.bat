REM Run from YADAMU_HOME
@set YADAMU_HOME=%CD%
@set YADAMU_TEST_HOME=%YADAMU_HOME%\tests
@set YADAMU_WORK_ID=import
@set YADAMU_WORK_ROOT=%YADAMU_HOME%\work\%YADAMU_WORK_ID%
if not exist %YADAMU_WORK_ROOT%\ mkdir %YADAMU_WORK_ROOT%
@set YADAMU_LOG_ROOT=%YADAMU_WORK_ROOT%\logs
call %YADAMU_TEST_HOME%\windows\initializeLogging.bat
call %YADAMU_TEST_HOME%\windows\installYadamu.bat
call %YADAMU_TEST_HOME%\windows\createDefaultUsers.bat
node %YADAMU_TEST_HOME%\node\yadamuTest CONFIG=%YADAMU_TEST_HOME%\%YADAMU_WORK_ID%\config.json >%YADAMU_LOG_PATH%\%YADAMU_WORK_ID%.log