@set CONFIGURATION_FILE=%1
call %~dp0runContainer.bat YADAMU-01 yadamu/regression:latest regression %CONFIGURATION_FILE% 
