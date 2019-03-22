@set MODE=DATA_ONLY
call ..\windows\initialize.bat %~dp1
set Y
call %YADAMU_SCRIPT_ROOT%\windows\export_Native_Schemas.bat
@set MODE=DATA_ONLY
call %YADAMU_SCRIPT_ROOT%\windows\clone_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\clone_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\windows\clone_MSSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\clone_MSSQL_ALL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_Oracle.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MSSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MSSQL_ALL.bat
@set MODE=DDL_AND_DATA
call %YADAMU_SCRIPT_ROOT%\windows\clone_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\windows\jTable_Oracle.bat 