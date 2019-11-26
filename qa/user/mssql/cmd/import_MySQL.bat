call qa\cmd\initialize.bat %~dp0 %~dp0 mysql import
@set YADAMU_PARSER=CLARINET
@set FILENAME=sakila
@set SCHEMA=sakila
@set SCHEMAVER=1
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_INPUT_PATH%\%FILENAME%.json to_user=\"dbo\" mode=%MODE%  log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"dbo\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
@set SCHEMAVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json to_user=\"dbo\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -vDATETIME_PRECISION=9 -vSPATIAL_PRECISION=18 -i%YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"dbo\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
@set FILENAME=jsonExample
@set SCHEMA=jtest
@set SCHEMAVER=1
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_INPUT_PATH%\%FILENAME%.json to_user=\"dbo\" mode=%MODE%  log_file=%YADAMU_IMPORT_LOG%
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"dbo\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
@set SCHEMAVER=2
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vMSSQL_SCHEMA=%SCHEMA%%SCHEMAVER% -i%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json to_user=\"dbo\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
sqlcmd -U%DB_USER% -P%DB_PWD% -S%DB_HOST% -d%DB_DBNAME% -I -e -vDATABASE=%DB_DBNAME% -vSCHEMA=%SCHEMA% -vID1=1 -vID2=%SCHEMAVER% -vMETHOD=%YADAMU_PARSER% -vDATETIME_PRECISION=9 -vSPATIAL_PRECISION=18 -i%YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=%SCHEMA%%SCHEMAVER% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"dbo\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_QA_BIN%\compareFileSizes %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH%
node %YADAMU_QA_BIN%\compareArrayContent %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH% false