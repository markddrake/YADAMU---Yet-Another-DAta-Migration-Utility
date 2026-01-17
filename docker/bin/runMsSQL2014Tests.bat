if defined %1 set  MSSQL14=%1
if not defined MSSQL14 set /p MSSQL14="SQL Server 2014 IP Address :"
call %~dp0runContainer.bat YADAMU-01 yadamu/regression:latest mssql14