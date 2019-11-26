call qa\cmd\initialize.bat $BASH_SOURCE[0] $BASH_SOURCE[0] mssql export
@set YADAMU_OUTPUT_BASE=%YADAMU_HOME%\JSON\
if not exist %YADAMU_OUTPUT_BASE%\ mkdir %YADAMU_OUTPUT_BASE%
@set YADAMU_OUTPUT_PATH=%YADAMU_OUTPUT_BASE%\%YADAMU_TARGET%
if not exist %YADAMU_OUTPUT_PATH%\ mkdir %YADAMU_OUTPUT_PATH%
@set MODE=DATA_ONLY
call %YADAMU_SCRIPT_PATH%\export_operations_MsSQL.bat %YADAMU_INPUT_PATH% "" "" %MODE%
@set FILENAME=AdventureWorksALL
@set SCHEMA=ADVWRK
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_SCRIPT_PATH%\import_operations_ADVWRK.bat %YADAMU_INPUT_PATH% "" "" 
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%YADAMU_INPUT_PATH%\%FILENAME%.json overwrite=yes from_user=\"dbo\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%