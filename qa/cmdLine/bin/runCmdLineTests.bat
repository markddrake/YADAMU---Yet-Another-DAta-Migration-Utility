@echo %YADAMU_TRACE%
set YADAMU_HOME=%CD%
set YADAMU_LOG_ROOT=%YADAMU_HOME%\log
set YADAMU_TESTNAME=cmdLine
set YADAMU_OUTPUT_PATH=%YADAMU_HOME%\%YADAMU_TESTNAME%
echo Output written to: %YADAMU_OUTPUT_PATH%
if exist %YADAMU_OUTPUT_PATH% rmdir /s /q %YADAMU_OUTPUT_PATH%
mkdir %YADAMU_OUTPUT_PATH%
call %YADAMU_HOME%\qa\bin\initializeLogging.bat %YADAMU_TESTNAME%
set YADAMU_LOG_FOLDER=%YADAMU_LOG_PATH%
echo Scripts logged to: %YADAMU_LOG_PATH%
echo Yadamu log file: %YADAMU_IMPORT_LOG%
REM Mode is set internally by the export_sample_datasets scripts
echo Exporting Oracle19c
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_oracle19c.log
call qa\cmdLine\oracle19c\bin\export_sample_datasets.bat 1>%SHELL_LOG_FILE% 2>&1
echo Exporting Oracle18c
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_oracle18c.log
call qa\cmdLine\oracle18c\bin\export_sample_datasets.bat 1>%SHELL_LOG_FILE% 2>&1
echo Exporting Oracle12c
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_oracle12c.log
call qa\cmdLine\oracle12c\bin\export_sample_datasets.bat 1>%SHELL_LOG_FILE% 2>&1
echo Exporting Oracle11g
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_oracle11g.log
call qa\cmdLine\oracle11g\bin\export_sample_datasets.bat 1>%SHELL_LOG_FILE% 2>&1
echo Exporting MsSQL Server 2017
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_mssql17.log
call qa\cmdLine\mssql17\bin\export_sample_datasets.bat   1>%SHELL_LOG_FILE% 2>&1
echo Exporting MsSQL Server 2019
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_mssql19.log
call qa\cmdLine\mssql19\bin\export_sample_datasets.bat   1>%SHELL_LOG_FILE% 2>&1
echo Exporting MySQL
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\export_mysql.log
call qa\cmdLine\mysql\bin\export_sample_datasets.bat     1>%SHELL_LOG_FILE% 2>&1
set MODE=DATA_ONLY
echo Testing Oracle19c
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\oracle19c.log
call qa\cmdLine\oracle19c\bin\cmdLineTests.bat           1>%SHELL_LOG_FILE% 2>&1
echo Testing Oracle18c
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\oracle18c.log
call qa\cmdLine\oracle18c\bin\cmdLineTests.bat           1>%SHELL_LOG_FILE% 2>&1
echo Testing Oracle12c
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\oracle12c.log
call qa\cmdLine\oracle12c\bin\cmdLineTests.bat           1>%SHELL_LOG_FILE% 2>&1
echo Testing Oracle11g
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\oracle11g.log
call qa\cmdLine\oracle11g\bin\cmdLineTests.bat           1>%SHELL_LOG_FILE% 2>&1
echo Testing MsSQL Server 2017
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\mssql17.log
call qa\cmdLine\mssql17\bin\cmdLineTests.bat             1>%SHELL_LOG_FILE% 2>&1
echo Testing MsSQL Server 2019
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\mssql19.log
call qa\cmdLine\mssql19\bin\cmdLineTests.bat             1>%SHELL_LOG_FILE% 2>&1
echo Testing Postgres
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\postgres.log
call qa\cmdLine\postgres\bin\cmdLineTests.bat            1>%SHELL_LOG_FILE% 2>&1
echo Testing MySQL
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\mysql.log
call qa\cmdLine\mysql\bin\cmdLineTests.bat               1>%SHELL_LOG_FILE% 2>&1
echo Testing MariaDB
set SHELL_LOG_FILE=%YADAMU_LOG_PATH%\mariadb.log
call qa\cmdLine\mariadb\bin\cmdLineTests.bat             1>%SHELL_LOG_FILE% 2>&1