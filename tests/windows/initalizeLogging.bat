if not exist %YADAMU_LOG_ROOT%\. mkdir %YADAMU_LOG_ROOT%
if not defined YADAMU_LOG_PATH call :SETLOGPATH
if not exist %YADAMU_LOG_PATH%\ mkdir %YADAMU_LOG_PATH%
if not exist %YADAMU_LOG_PATH%\install\ mkdir %YADAMU_LOG_PATH%\install
@set IMPORTLOG=%YADAMU_LOG_PATH%\yadamu.log
@set EXPORTLOG=%YADAMU_LOG_PATH%\yadamu.log
exit /b

:SETLOGPATH
call %YADAMU_HOME%\tests\windows\getUTCTime.bat
@set YADAMU_LOG_PATH=%YADAMU_LOG_ROOT%\%UTC%
exit /b