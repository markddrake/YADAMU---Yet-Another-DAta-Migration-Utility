REM Run from YADAMU_HOME
@set YADAMU_HOME=%CD%
@set YADAMU_QA_HOME=%YADAMU_HOME%\qa
@set YADAMU_TASK=dbRoundtrip
call %YADAMU_HOME%\app\install\cmd\installYadamu.bat
call %YADAMU_QA_HOME%\install\cmd\installYadamu.bat
@set "YADAMU_LOG_PATH="
@set YADAMU_LOG_ROOT=%YADAMU_HOME%\log
call %YADAMU_QA_HOME%\cmd\initializeLogging.bat %YADAMU_TASK%
node %YADAMU_HOME%\app\YADAMU\common\test CONFIG=%YADAMU_QA_HOME%\regression\%YADAMU_TASK%.json >%YADAMU_LOG_PATH%\%YADAMU_TASK%.log
