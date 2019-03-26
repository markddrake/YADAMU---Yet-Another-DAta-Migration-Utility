call ..\windows\initialize.bat %~dp1
call %YADAMU_SCRIPT_ROOT%\windows\clone_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\clone_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\windows\clone_MSSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MYSQL.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_Oracle.bat
call %YADAMU_SCRIPT_ROOT%\windows\jTable_MSSQL.bat
@set MODE=DDL_AND_DATA
call %YADAMU_SCRIPT_ROOT%\windows\clone_Oracle.bat 
call %YADAMU_SCRIPT_ROOT%\windows\jTable_Oracle.bat 