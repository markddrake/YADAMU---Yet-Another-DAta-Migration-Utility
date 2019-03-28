call tests/unix/yadamuTestEnv.sh
call :INITLOGGING
export SCHVER=1
sh $YADAMU_HOME/tests/oracle/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_MSSQL_ALL.sql $YADAMU_LOG_PATH 1 Clarinet
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_ORACLE_ALL.sql $YADAMU_LOG_PATH 1 Clarinet DDL_AND_DATA
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH JTEST1 Clarinet
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH SAKILA1 Clarinet
sh $YADAMU_HOME/tests/mssql/env/dbConnection.sh
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_HOME/mssql/sql/JSON_IMPORT.sql > $YADAMU_LOG_PATH/JSON_IMPORT_MSSQL.log
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -vID=1 -vMETHOD=Clarinet -i$YADAMU_HOME/tests/mssql/sql/RECREATE_MSSQL_ALL.sql >$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -vID=1 -vMETHOD=Clarinet -i$YADAMU_HOME/tests/mssql/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -vSCHEMA=JTEST -vID=1 -vMETHOD=Clarinet -i$YADAMU_HOME/tests/mssql/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/RECREATE_SCHEMA.log
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -vSCHEMA=SAKILA -vID=1 -vMETHOD=Clarinet -i$YADAMU_HOME/tests/mssql/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/RECREATE_SCHEMA.log
export SCHEMA=JTEST
sh $YADAMU_HOME/tests/mysql/env/dbConnection.sh
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/mysql/sql/JSON_IMPORT.sql >$YADAMU_LOG_PATH/JSON_IMPORT_MYSQL.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @ID='1'; then export @METHOD='Clarinet'" <$YADAMU_HOME/tests/mysql/sql/RECREATE_MSSQL_ALL.sql >$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @ID='1'; then export @METHOD='Clarinet';"<$YADAMU_HOME/tests/mysql/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @SCHEMA='JTEST'; then export @ID='1'; then export @METHOD='Clarinet'" <$YADAMU_HOME/tests/mariadb/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @SCHEMA='SAKILA'; then export @ID='1'; then export @METHOD='Clarinet'" <$YADAMU_HOME/tests/mariadb/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
sh $YADAMU_HOME/tests/mariadb/env/dbConnection.sh
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/mariadb/sql/SCHEMA_COMPARE.sql >$YADAMU_LOG_PATH/SCHEMA_COMPARE_MARIADB.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @ID='1'; then export @METHOD='Clarinet'" <$YADAMU_HOME/tests/mysql/sql/RECREATE_MSSQL_ALL.sql >$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @ID='1'; then export @METHOD='Clarinet';"<$YADAMU_HOME/tests/mysql/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @SCHEMA='JTEST'; then export @ID='1'; then export @METHOD='Clarinet'" <$YADAMU_HOME/tests/mariadb/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="export @SCHEMA='SAKILA'; then export @ID='1'; then export @METHOD='Clarinet'" <$YADAMU_HOME/tests/mariadb/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
sh $YADAMU_HOME/tests/postgres/env/dbConnection.sh
psql -U $DB_USER -h $DB_HOST -a -f $YADAMU_HOME/postgres/sql/JSON_IMPORT.sql > $YADAMU_LOG_PATH/JSON_IMPORT_POSTGRES.log
psql -U $DB_USER -h $DB_HOST -a -vID=1 -vMETHOD=Clarinet -f $YADAMU_HOME/tests/postgres/sql/RECREATE_MSSQL_ALL.sql >$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
psql -U $DB_USER -h $DB_HOST -a -vID=1 -vMETHOD=Clarinet -f $YADAMU_HOME/tests/postgres/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
psql -U $DB_USER -h $DB_HOST -a -vSCHEMA=JTEST -vID=1 -vMETHOD=Clarinet -f $YADAMU_HOME/tests/postgres/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/RECREATE_SCHEMA.log
psql -U $DB_USER -h $DB_HOST -a -vSCHEMA=SAKILA -vID=1 -vMETHOD=Clarinet -f $YADAMU_HOME/tests/postgres/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/RECREATE_SCHEMA.log
exit /b

:INITLOGGING
sh $YADAMU_HOME/tests/unix/getUTCTime.sh
export YADAMU_LOG=$YADAMU_HOME/logs
mkdir $YADAMU_LOG
export YADAMU_LOG=$YADAMU_LOG/$UTC
mkdir $YADAMU_LOG
rmdir /q /s $YADAMU_LOG
mkdir $YADAMU_LOG
mkdir $YADAMU_LOG/install
export LOGDIR=$YADAMU_LOG
exit /b
