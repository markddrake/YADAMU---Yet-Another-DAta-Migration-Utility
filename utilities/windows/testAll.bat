cls
set YADAMU_HOME=%CD%
call utilities\windows\getUTCTime.bat
mkdir logs
set YADAMU_LOG=%YADAMU_HOME%\logs\%UTC%
mkdir %YADAMU_LOG%
call :EXPORT_MASTER
call :ORACLE18c
call :MSSQL
call :POSTGRES
call :MySQL
call :MARIADB
call :ORACLE12c
exit /b

:EXPORT_MASTER
cd %YADAMU_HOME%
set TEST_NAME="export"
cd oracle\tests
call windows\export_Master 1> %YADAMU_LOG%\Export.log  2>&1
cd %YADAMU_HOME%
cd oracle\ORCL12c
call windows\export_Master 1>> %YADAMU_LOG%\Export.log  2>&1
cd %YADAMU_HOME%
set mode=DATA_ONLY
cd mssql\tests
call windows\export_Master 1>> %YADAMU_LOG%\Export.log  2>&1
cd %YADAMU_HOME%
set mode=DATA_ONLY
cd mysql\tests
call windows\export_Master 1>> %YADAMU_LOG%\Export.log  2>&1
exit /b

:ORACLE18c
cd %YADAMU_HOME%
cd  oracle\tests
call windows\testAll 1> %YADAMU_LOG%\Oracle18c.log 2>&1
exit /b

:POSTGRES
cd %YADAMU_HOME%
cd postgres\tests
call windows\testAll 1> %YADAMU_LOG%\Postgres.log  2>&1
exit /b

:MSSQL
cd %YADAMU_HOME%
cd mssql\tests
call windows\testAll 1> %YADAMU_LOG%\MsSQL.log  2>&1
exit /b

:MYSQL
cd %YADAMU_HOME%
cd mysql\tests
call windows\testAll 1> %YADAMU_LOG%\MySQL.log  2>&1
exit /b

:MARIADB
cd %YADAMU_HOME%
cd MariaDB\tests
call windows\testAll 1> %YADAMU_LOG%\MariaDB.log  2>&1
exit /b

:ORACLE12c
cd %YADAMU_HOME%
cd oracle\ORCL12c
call windows\testAll 1> %YADAMU_LOG%\Oracle12c.log  2>&1
exit /b