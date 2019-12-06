call %YADAMU_HOME%\app\install\mssql\env\dbConnection.bat
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=AdventureWorksAll -i%YADAMU_QA_HOME%\sql\mssql\RECREATE_SCHEMA.sql >> %YADAMU_LOG_PATH%\MSSQL_RECREATE_SCHEMA.log
