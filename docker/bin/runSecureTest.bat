@set CONFIGURATION_FILE=%1
call %~dp0runContainer.bat YADAMU-01 yadamu/secure:latest regression %CONFIGURATION_FILE%