@set YADAMU_HOME=%CD%
@set YADAMU_OUTPUT_NAME=ExportRoundtrip
call tests\windows\installYadamu
call tests\createLogDirectory
call :cleandir mariadb
call :cleandir mssql
call :cleandir mysql
call :cleandir oracle
call :cleandir oracle12c
call :cleandir postgres
node %YADAMU_HOME%\tests\node\yadamuTest CONFIG=%YADAMU_HOME%\tests\%YADAMU_OUTPUT_NAME%\config.json >%YADAMU_LOG_PATH%\%YADAMU_OUTPUT_NAME%.log
exit /b

:cleandir
rmdir /s /q tests\%1\JSON
mkdir tests\%1\JSON
mkdir tests\%1\JSON\MariaDB
mkdir tests\%1\JSON\MsSQL
mkdir tests\%1\JSON\MySQL
mkdir tests\%1\JSON\ORCL18c
mkdir tests\%1\JSON\ORCL12c
mkdir tests\%1\JSON\Postgres
mkdir tests\%1\JSON\ORCL18c\DDL_ONLY
mkdir tests\%1\JSON\ORCL18c\DATA_ONLY
mkdir tests\%1\JSON\ORCL18c\DDL_AND_DATA
mkdir tests\%1\JSON\ORCL12c\DDL_ONLY
mkdir tests\%1\JSON\ORCL12c\DATA_ONLY
mkdir tests\%1\JSON\ORCL12c\DDL_AND_DATA
exit /b