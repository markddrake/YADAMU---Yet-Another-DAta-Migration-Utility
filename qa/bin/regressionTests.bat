REM Run from YADAMU_HOME
@set YADAMU_HOME=%CD%
@set YADAMU_QA_HOME=%YADAMU_HOME%\qa
call %YADAMU_QA_HOME%\bin\export.bat
call %YADAMU_QA_HOME%\bin\import.bat
call %YADAMU_QA_HOME%\bin\dbRoundtrip.bat
call %YADAMU_QA_HOME%\bin\fileRoundtrip.bat
