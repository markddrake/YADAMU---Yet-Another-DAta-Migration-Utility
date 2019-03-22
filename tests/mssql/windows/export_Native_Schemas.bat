if not defined MODE set MODE=DATA_ONLY
@set YADAMU_TARGET=MsSQL
call ..\windows\initialize.bat %~dp0
if not exist %YADAMU_INPUT_PATH%\ mkdir %YADAMU_INPUT_PATH%
call %YADAMU_SCRIPT_ROOT%\windows\export_MSSQL.bat %YADAMU_INPUT_PATH% "" ""
