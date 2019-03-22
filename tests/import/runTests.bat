@set YADAMU_HOME=%CD%
@set YADAMU_TEST_NAME=Import
call tests\windows\createLogDir.bat
call tests\windows\installYadamu.bat
call tests\windows\createDefaultUsers.bat
node %YADAMU_HOME%\tests\node\yadamuTest CONFIG=%YADAMU_HOME%\tests\%YADAMU_TEST_NAME%\config.json >%YADAMU_LOG_PATH%\%YADAMU_TEST_NAME%.log