source qa/sh/initialize.sh $BASH_SOURCE[0] $BASH_SOURCE[0] mssql export
export YADAMU_OUTPUT_BASE=$YADAMU_HOME\JSON\
if [ ! -e $YADAMU_OUTPUT_BASE ]; then mkidr -p $YADAMU_OUTPUT_BASE; fi
export YADAMU_OUTPUT_PATH=$YADAMU_OUTPUT_BASE%\$YADAMU_TARGET
if [ ! -e $YADAMU_OUTPUT_PATH ]; then mkidr -p $YADAMU_OUTPUT_PATH; fi
export FILENAME=AdventureWorksALL
export MSSQL_SCHEMA=ADVWRK
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
call %YADAMU_SCRIPT_PATH%\import_MSSQL_ALL.bat %YADAMU_INPUT_PATH% "" "" 
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA% file=%YADAMU_OUTPUT_PATH%\%FILENAME%.json overwrite=yes from_user=\"dbo\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%