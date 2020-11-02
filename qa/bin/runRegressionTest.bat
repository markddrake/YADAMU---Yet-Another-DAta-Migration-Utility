REM Run from YADAMU_HOME
@set YADAMU_TASK=%1
@set YADAMU_HOME=%CD%
@set YADAMU_QA_HOME=%YADAMU_HOME%\qa
call %YADAMU_HOME%\src\install\bin\installYadamu.bat
call %YADAMU_QA_HOME%\install\bin\installYadamu.bat
@set "YADAMU_LOG_PATH="
@set YADAMU_LOG_ROOT=%YADAMU_HOME%\log
call %YADAMU_QA_HOME%\bin\initializeLogging.bat %YADAMU_TASK%
if not defined NODE_NO_WARNINGS set NODE_NO_WARNINGS=1
node %YADAMU_HOME%\src\YADAMU_QA\common\node\test.js CONFIG=%YADAMU_QA_HOME%\regression\%YADAMU_TASK%.json EXCEPTION_FOLDER=%YADAMU_LOG_PATH%>%YADAMU_LOG_PATH%\%YADAMU_TASK%.log