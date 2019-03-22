@set YADAMU_TEST_NAME=dbRoundtrip
@set YADAMU_HOME=%CD%
call tests\windows\installYadamu
node %YADAMU_HOME%\tests\node\yadamuTest CONFIG=%YADAMU_HOME%\tests\%YADAMU_TEST_NAME%\config.json >%YADAMU_LOG_PATH%\%YADAMU_TEST_NAME%.log
exit /b