cls
set YADAMU_HOME=%CD%
set YADAMU_LOG_ROOT=%YADAMU_HOME%\work\logs
set YADAMU_LOG_PATH=
call %YADAMU_HOME%\tests\windows\initalizeLogging.bat
call :EXPORT_NATIVE_SCHEMAS
call :ORACLE18c
call :MSSQL
call :POSTGRES
call :MySQL
call :MARIADB
call :ORACLE12c
exit /b

:EXPORT_NATIVE_SCHEMAS
cd %YADAMU_HOME%
set TEST_NAME="export"
cd tests\oracle18c
call windows\export_Native_Schemas.bat 1> %YADAMU_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%
cd tests\oracle12c
call windows\export_Native_Schemas.bat 1>> %YADAMU_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%
cd tests\mssql
call windows\export_Native_Schemas.bat 1>> %YADAMU_LOG_PATH%\Export.log  2>&1
cd %YADAMU_HOME%
set mode=DATA_ONLY
cd tests\mysql
call windows\export_Native_Schemas.bat 1>> %YADAMU_LOG_PATH%\Export.log  2>&1
exit /b

:ORACLE18c
cd %YADAMU_HOME%
cd tests\oracle
call %YADAMU_HOME%\tests\windows\testAll.bat 1> %YADAMU_LOG_PATH%\Oracle18c.log 2>&1
exit /b

:POSTGRES
cd %YADAMU_HOME%
cd tests\postgres
call %YADAMU_HOME%\tests\windows\testAll.bat 1> %YADAMU_LOG_PATH%\Postgres.log  2>&1
exit /b

:MSSQL
cd %YADAMU_HOME%
cd tests\mssql
call %YADAMU_HOME%\tests\windows\testAll.bat 1> %YADAMU_LOG_PATH%\MsSQL.log  2>&1
exit /b

:MYSQL
cd %YADAMU_HOME%
cd tests\mysql
call %YADAMU_HOME%\tests\windows\testAll.bat 1> %YADAMU_LOG_PATH%\MySQL.log  2>&1
exit /b

:MARIADB
cd %YADAMU_HOME%
cd tests\mariaDB
call %YADAMU_HOME%\tests\windows\testAll.bat 1> %YADAMU_LOG_PATH%\MariaDB.log  2>&1
exit /b

:ORACLE12c
cd %YADAMU_HOME%
cd tests\oracle\ORCL12c
call %YADAMU_HOME%\tests\windows\testAll.bat 1> %YADAMU_LOG_PATH%\Oracle12c.log  2>&1
exit /b