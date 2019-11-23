cls
@set YADAMU_HOME=%CD%
@set YADAMU_LOG_ROOT=%YADAMU_HOME%\work\logs
@set YADAMU_LOG_PATH=
call %YADAMU_HOME%\tests\cmd\initializeLogging.bat
@set MASTER_LOG_PATH=%YADAMU_LOG_PATH%
call :EXPORT_NATIVE_SCHEMAS
call :ORACLE19c
call :ORACLE18c
call :ORACLE12c
call :ORACLE11g
call :MSSQL
call :POSTGRES
call :MySQL
call :MARIADB
cd %YADAMU_HOME%
exit /b

:EXPORT_NATIVE_SCHEMAS
@set YADAMU_LOG_ROOT=%YADAMU_HOME%\work\export
call %YADAMU_HOME%\tests\cmd\initializeLogging.bat
cd %YADAMU_HOME%\tests\oracle19c
call cmd\export_Native_Schemas.bat 1> %MASTER_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%\tests\oracle18c
call cmd\export_Native_Schemas.bat 1> %MASTER_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%\tests\oracle12c
call cmd\export_Native_Schemas.bat 1>> %MASTER_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%\tests\oracle11g
call cmd\export_Native_Schemas.bat 1>> %MASTER_LOG_PATH%\Export.log  2>&1
@set mode=DATA_ONLY
cd %YADAMU_HOME%\tests\mssql
call cmd\export_Native_Schemas.bat 1>> %MASTER_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%\tests\mysql
call cmd\export_Native_Schemas.bat 1>> %MASTER_LOG_PATH%\Export.log  2>&1
exit /b

:ORACLE19c
cd %YADAMU_HOME%\tests\oracle19c
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\Oracle19c.log 2>&1
exit /b

:ORACLE18c
cd %YADAMU_HOME%\tests\oracle18c
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\Oracle18c.log 2>&1
exit /b

:ORACLE12c
cd %YADAMU_HOME%\tests\oracle12c
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\Oracle12c.log  2>&1
exit /b

:ORACLE11g
cd %YADAMU_HOME%\tests\oracle11g
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\Oracle11g.log  2>&1
exit /b

:MSSQL
cd %YADAMU_HOME%\tests\mssql
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\MsSQL.log  2>&1
exit /b

:POSTGRES
cd %YADAMU_HOME%\tests\postgres
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\Postgres.log  2>&1
exit /b

:MYSQL
cd %YADAMU_HOME%\tests\mysql
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\MySQL.log  2>&1
exit /b

:MARIADB
cd %YADAMU_HOME%\tests\mariaDB
@set YADAMU_LOG_PATH=
call cmd\runAllTests.bat 1> %MASTER_LOG_PATH%\MariaDB.log  2>&1
exit /b

