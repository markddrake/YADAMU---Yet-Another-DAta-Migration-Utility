if defined %1 set  MSSQL12%1
if not defined MSSQL12 set /p MSSQL12="SQL Server 2014 IP Address :"
call %~dp0runContainer.bat YADAMU-01 yadamu/regression:latest mssql12