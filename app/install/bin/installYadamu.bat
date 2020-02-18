REM If running manally, run from the YADAMU 'home' folder.
if not defined YADAMU_HOME @set YADAMU_HOME=%CD%
@set YADAMU_INSTALL=%YADAMU_HOME%\app\install
@set YADAMU_LOG_PATH=%YADAMU_HOME%\log
if not exist %YADAMU_LOG_PATH%\ mkdir %YADAMU_LOG_PATH%
@set YADAMU_LOG_PATH=%YADAMU_LOG_PATH%\install
if exist %YADAMU_LOG_PATH%\ rmdir /s /q %YADAMU_LOG_PATH%
@set YADAMU_DB=oracle19c
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_INSTALL%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%\%YADAMU_DB%
@set YADAMU_DB=oracle18c
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_INSTALL%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%\%YADAMU_DB%
@set YADAMU_DB=oracle12c
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_INSTALL%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%\%YADAMU_DB%
@set YADAMU_DB=oracle11g
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @%YADAMU_INSTALL%\oracle\sql\COMPILE_ALL.sql %YADAMU_LOG_PATH%\%YADAMU_DB%
@set YADAMU_DB=mssql
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -dmaster -I -e -i%YADAMU_INSTALL%\mssql\sql\YADAMU_IMPORT.sql > %YADAMU_LOG_PATH%\%YADAMU_DB%\YADAMU_IMPORT.log
@set YADAMU_DB=mysql
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
mysql.exe -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% -v -f <%YADAMU_INSTALL%\mysql\sql\YADAMU_IMPORT.sql >%YADAMU_LOG_PATH%\%YADAMU_DB%\YADAMU_IMPORT.log
@set YADAMU_DB=postgres
mkdir %YADAMU_LOG_PATH%\%YADAMU_DB%
call %YADAMU_INSTALL%\%YADAMU_DB%\env\dbConnection.bat
psql -U %DB_USER% -d %DB_DBNAME% -h %DB_HOST% -a  -f %YADAMU_INSTALL%\postgres\sql\YADAMU_IMPORT.sql > %YADAMU_LOG_PATH%\%YADAMU_DB%\YADAMU_IMPORT.log
