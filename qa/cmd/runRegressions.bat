REM Run from YADAMU_HOME
@set YADAMU_HOME=%CD%
@set YADAMU_QA_HOME=%YADAMU_HOME%\qa
call %YADAMU_QA_HOME%\cmd\export.bat
call %YADAMU_QA_HOME%\cmd\import.bat
call %YADAMU_QA_HOME%\cmd\dbRoundtrip.bat
call %YADAMU_QA_HOME%\cmd\fileRoundtrip.bat
