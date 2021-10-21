@set CONFIGURATION_FILE=%1
call %~dp0runContainer.bat YADAMU-01 yadamu/regression:latest custom %CONFIGURATION_FILE%
