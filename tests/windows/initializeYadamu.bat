call tests\windows\yadamuTestEnv.bat
call :INITLOGGING
@set SCHVER=1
call %YADAMU_TEST_HOME%\oracle19c\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_HOME%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\\oracle\sql\RECREATE_MSSQL_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_ORACLE_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet DDL_AND_DATA
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% jtest1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% sakila1 Clarinet
call %YADAMU_TEST_HOME%\oracle18c\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_HOME%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\\oracle\sql\RECREATE_MSSQL_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_ORACLE_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet DDL_AND_DATA
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% jtest1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% sakila1 Clarinet
call %YADAMU_TEST_HOME%\oracle12c\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_HOME%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\\oracle\sql\RECREATE_MSSQL_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_ORACLE_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet DDL_AND_DATA
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% jtest1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% sakila1 Clarinet
call %YADAMU_TEST_HOME%\oracle11g\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_HOME%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\\oracle\sql\RECREATE_MSSQL_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_ORACLE_ALL.sql %YADAMU_LOG_PATH% 1 Clarinet DDL_AND_DATA
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% jtest1 Clarinet
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_TEST_HOME%\oracle\sql\RECREATE_SCHEMA.sql %YADAMU_LOG_PATH% sakila1 Clarinet
call %YADAMU_TEST_HOME%\mssql\env\dbConnection.bat
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -i%YADAMU_HOME%\mssql\sql\YADAMU_IMPORT.sql > %YADAMU_LOG_PATH%\MSSQL_YADAMU_IMPORT.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=1 -vMETHOD=Clarinet -i%YADAMU_TEST_HOME%\mssql\sql\RECREATE_MSSQL_ALL.sql >%YADAMU_LOG_PATH%\MSSQL_RECREATE_SCHEMA.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vID=1 -vMETHOD=Clarinet -i%YADAMU_TEST_HOME%\mssql\sql\RECREATE_ORACLE_ALL.sql >>%YADAMU_LOG_PATH%\MSSQL_RECREATE_SCHEMA.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=jtest -vID=1 -vMETHOD=Clarinet -i%YADAMU_TEST_HOME%\mssql\sql\RECREATE_SCHEMA.sql >> %YADAMU_LOG_PATH%\MSSQL_RECREATE_SCHEMA.log
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vSCHEMA=sakila -vID=1 -vMETHOD=Clarinet -i%YADAMU_TEST_HOME%\mssql\sql\RECREATE_SCHEMA.sql >> %YADAMU_LOG_PATH%\MSSQL_RECREATE_SCHEMA.log
@set SCHEMA=jtest
call %YADAMU_TEST_HOME%\mysql\env\dbConnection.bat
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <%YADAMU_HOME%\mysql\sql\YADAMU_IMPORT.sql >%YADAMU_LOG_PATH%\MYSQL_YADAMU_IMPORT.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @ID='1'; set @METHOD='Clarinet'" <%YADAMU_TEST_HOME%\mysql\sql\RECREATE_MSSQL_ALL.sql >%YADAMU_LOG_PATH%\MYSQL_RECREATE_SCHEMA.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @ID='1'; set @METHOD='Clarinet';"<%YADAMU_TEST_HOME%\mysql\sql\RECREATE_ORACLE_ALL.sql >>%YADAMU_LOG_PATH%\MYSQL_RECREATE_SCHEMA.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @SCHEMA='jtest'; set @ID='1'; set @METHOD='Clarinet'" <%YADAMU_TEST_HOME%\mariadb\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\MYSQL_RECREATE_SCHEMA.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @SCHEMA='sakila'; set @ID='1'; set @METHOD='Clarinet'" <%YADAMU_TEST_HOME%\mariadb\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\MYSQL_RECREATE_SCHEMA.log
call %YADAMU_TEST_HOME%\mariadb\env\dbConnection.bat
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <%YADAMU_HOME%\mariadb\sql\YADAMU_IMPORT.sql >%YADAMU_LOG_PATH%\MARIADB_YADAMU_IMPORT.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @ID='1'; set @METHOD='Clarinet'" <%YADAMU_TEST_HOME%\mysql\sql\RECREATE_MSSQL_ALL.sql >%YADAMU_LOG_PATH%\MARIADB_RECREATE_SCHEMA.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @ID='1'; set @METHOD='Clarinet';"<%YADAMU_TEST_HOME%\mysql\sql\RECREATE_ORACLE_ALL.sql >>%YADAMU_LOG_PATH%\MARIADB_RECREATE_SCHEMA.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @SCHEMA='jtest'; set @ID='1'; set @METHOD='Clarinet'" <%YADAMU_TEST_HOME%\mariadb\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\MARIADB_RECREATE_SCHEMA.log
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f --init-command="set @SCHEMA='sakila'; set @ID='1'; set @METHOD='Clarinet'" <%YADAMU_TEST_HOME%\mariadb\sql\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\MARIADB_RECREATE_SCHEMA.log
call %YADAMU_TEST_HOME%\postgres\env\dbConnection.bat
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -f %YADAMU_HOME%\postgres\sql\YADAMU_IMPORT.sql > %YADAMU_LOG_PATH%\POSTGRES_YADAMU_IMPORT.log
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vID=1 -vMETHOD=Clarinet -f %YADAMU_TEST_HOME%\postgres\sql\RECREATE_MSSQL_ALL.sql >%YADAMU_LOG_PATH%\POSTGRES_RECREATE_SCHEMA.log
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vID=1 -vMETHOD=Clarinet -f %YADAMU_TEST_HOME%\postgres\sql\RECREATE_ORACLE_ALL.sql >>%YADAMU_LOG_PATH%\POSTGRES_RECREATE_SCHEMA.log
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vSCHEMA=jtest -vID=1 -vMETHOD=Clarinet -f %YADAMU_TEST_HOME%\postgres\sql\RECREATE_SCHEMA.sql >> %YADAMU_LOG_PATH%\POSTGRES_RECREATE_SCHEMA.log
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a -vSCHEMA=sakila -vID=1 -vMETHOD=Clarinet -f %YADAMU_TEST_HOME%\postgres\sql\RECREATE_SCHEMA.sql >> %YADAMU_LOG_PATH%\POSTGRES_RECREATE_SCHEMA.log
exit /b

:INITLOGGING
call %YADAMU_TEST_HOME%\windows\getUTCTime.bat
@set YADAMU_LOG=%YADAMU_HOME%\logs
mkdir %YADAMU_LOG%
@set YADAMU_LOG=%YADAMU_LOG%\%UTC%
mkdir %YADAMU_LOG%
rmdir /q /s %YADAMU_LOG%
mkdir %YADAMU_LOG%
mkdir %YADAMU_LOG%\install
@set LOGDIR=%YADAMU_LOG%
exit /b
