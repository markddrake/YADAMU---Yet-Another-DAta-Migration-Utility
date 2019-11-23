REM %1 is the path to the script that initiated this process
REM %2 is the path to the script that called this script.
REM %3 is the RDBMS name required to calculate the location of input and output folders.
if not defined YADAMU_HOME set YADAMU_HOME=%CD%
if not defined MODE set MODE=DATA_ONLY
@set YADAMU_SOURCE=%3
REM append MODE to YADAMU_SOURCE if it was not set when calling initialize (eg YADAMU_SOURCE has a trailing '/'
@set YADAMU_TESTNAME=%4
for %%I in (%~dp1..) do set YADAMU_TARGET=%%~nxI
for %%I in (%~dp2..) do set YADAMU_DB=%%~nxI
@set YADAMU_BIN=%YADAMU_HOME%\app\YADAMU\common
@set YADAMU_QA_BIN=%YADAMU_HOME%\app\YADAMU_QA\utilities\node
@set YADAMU_ENV_PATH=%YADAMU_HOME%\app\install\%YADAMU_TARGET%
@set YADAMU_SQL_PATH=%YADAMU_HOME%\qa\sql\%YADAMU_DB%
@set YADAMU_SCRIPT_PATH=%~dp2
@set YADAMU_SCRIPT_PATH=%YADAMU_SCRIPT_PATH:~0,-1%
REM JSON Files are in {YADAMU_HOME}\JSON\{RDBMS} or for Oracle {YADAMU_HOME\JSON\{SOURCE}\{MODE}
@set YADAMU_JSON_BASE=%YADAMU_HOME%\JSON
@set YADAMU_INPUT_PATH=%YADAMU_JSON_BASE%\%YADAMU_SOURCE%
if [%YADAMU_SOURCE%]==[oracle] @set YADAMU_INPUT_PATH=%YADAMU_JSON_BASE%\%YADAMU_TARGET%\%MODE%
@set YADAMU_TEST_BASE=%YADAMU_HOME%\results
if not exist %YADAMU_TEST_BASE%\. mkdir %YADAMU_TEST_BASE%
if defined YADAMU_TESTNAME @set YADAMU_TEST_BASE=%YADAMU_TEST_BASE%\%YADAMU_TESTNAME%
if not exist %YADAMU_TEST_BASE%\. mkdir %YADAMU_TEST_BASE%
@set YADAMU_TEST_BASE=%YADAMU_TEST_BASE%\%YADAMU_TARGET%
if not exist %YADAMU_TEST_BASE%\. call %~dp0createOutputFolders.bat %YADAMU_TEST_BASE%
@set YADAMU_OUTPUT_PATH=%YADAMU_TEST_BASE%\JSON\%YADAMU_SOURCE%
if [%YADAMU_SOURCE%]==[oracle] @set YADAMU_OUTPUT_PATH=%YADAMU_TEST_BASE%\JSON\%YADAMU_TARGET%\%MODE%
if not exist %YADAMU_TEST_BASE%\. call %~dp0createOutputFolders.bat %YADAMU_TEST_BASE%
if exist %YADAMU_OUTPUT_PATH%\. rmdir /s /q %YADAMU_OUTPUT_PATH%\*
@set YADAMU_LOG_ROOT=%YADAMU_HOME%\log
call %~dp0initializeLogging.bat %YADAMU_TESTNAME%
call %YADAMU_ENV_PATH%\env\dbConnection.bat
SET Y
exit /b