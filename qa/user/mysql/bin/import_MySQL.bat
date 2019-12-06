call qa\bin\initialize.bat %~dp0 %~dp0 mysql import
@set YADAMU_PARSER=CLARINET
@set FILENAME=sakila
@set SCHEMA=sakila
@set SCHEMAVER=1
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT%  -v -f --init-command="set @SCHEMA='%SCHEMA%%SCHEMAVER%'; set @METHOD='%YADAMU_PARSER%'" <%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_INPUT_PATH%\%FILENAME%.json to_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_IMPORT_LOG%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% --init-command="set @SCHEMA='%SCHEMA%'; set @ID1=''; set @ID2=%SCHEMAVER%; set @METHOD='%YADAMU_PARSER%'" --table  <%YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
@set SCHEMAVER=2
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT%  -v -f --init-command="set @SCHEMA='%SCHEMA%%SCHEMAVER%'; set @METHOD='%YADAMU_PARSER%'" <%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json to_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% --init-command="set @SCHEMA='%SCHEMA%'; set @ID1=1; set @ID2=%SCHEMAVER%; set @METHOD='%YADAMU_PARSER%'" --table  <%YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
@set FILENAME=jsonExample
@set SCHEMA=jtest
@set SCHEMAVER=1
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT%  -v -f --init-command="set @SCHEMA='%SCHEMA%%SCHEMAVER%'; set @METHOD='%YADAMU_PARSER%'" <%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_INPUT_PATH%\%FILENAME%.json to_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE%  log_file=%YADAMU_IMPORT_LOG%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% --init-command="set @SCHEMA='%SCHEMA%'; set @ID1=''; set @ID2=%SCHEMAVER%; set @METHOD='%YADAMU_PARSER%'" --table  <%YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
@set SCHEMAVER=2
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT%  -v -f --init-command="set @SCHEMA='%SCHEMA%%SCHEMAVER%'; set @METHOD='%YADAMU_PARSER%'" <%YADAMU_SQL_PATH%\RECREATE_SCHEMA.sql >>%YADAMU_LOG_PATH%\RECREATE_SCHEMA.log
node %YADAMU_BIN%\import --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_OUTPUT_PATH%\%FILENAME%1.json to_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_IMPORT_LOG%
mysql -u%DB_USER% -p%DB_PWD% -h%DB_HOST% -D%DB_DBNAME% -P%DB_PORT% --init-command="set @SCHEMA='%SCHEMA%'; set @ID1=''; set @ID2=%SCHEMAVER%; set @METHOD='%YADAMU_PARSER%'" --table  <%YADAMU_SQL_PATH%\COMPARE_SCHEMA.sql >>%YADAMU_LOG_PATH%\COMPARE_SCHEMA.log
node %YADAMU_BIN%\export --rdbms=%YADAMU_DB% --username=%DB_USER% --hostname=%DB_HOST% --port=%DB_PORT% --password=%DB_PWD% --database=%DB_DBNAME% file=%YADAMU_OUTPUT_PATH%\%FILENAME%%SCHEMAVER%.json overwrite=yes from_user=\"%SCHEMA%%SCHEMAVER%\" mode=%MODE% log_file=%YADAMU_EXPORT_LOG%
node %YADAMU_QA_BIN%\compareFileSizes %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH%
node %YADAMU_QA_BIN%\compareArrayContent %YADAMU_LOG_PATH% %YADAMU_INPUT_PATH% %YADAMU_OUTPUT_PATH% false