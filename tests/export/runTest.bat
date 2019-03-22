@set YADAMU_HOME=%CD%
@set YADAMU_OUTPUT_NAME=Export
call tests\windows\installYadamu,bat
call tests\windows\createLogDirectory.bat
call tests\windows\createOutputFolders.bat %YADAMU_HOME%
node %YADAMU_HOME%\tests\node\yadamuTest CONFIG=%YADAMU_HOME%\tests\%YADAMU_OUTPUT_NAME%\config.json >%YADAMU_LOG_PATH%\%YADAMU_OUTPUT_NAME%.log

