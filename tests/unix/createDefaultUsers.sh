source $YADAMU_HOME/tests/oracle1                       c/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_MSSQL_ALL.sql $YADAMU_LOG_PATH ""
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH jtest
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH sakila
source $YADAMU_HOME/tests/oracle18c/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_MSSQL_ALL.sql $YADAMU_LOG_PATH ""
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH jtest
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH sakila
source $YADAMU_HOME/tests/oracle12c/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_MSSQL_ALL.sql $YADAMU_LOG_PATH ""
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH jtest
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/RECREATE_SCHEMA.sql $YADAMU_LOG_PATH sakila
source $YADAMU_HOME/tests/mssql/env/dbConnection.sh
echo ":setvar ID ''" > setvars.sql
export SQLCMDINI=setvars.sql
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_HOME/tests/mssql/sql/RECREATE_ORACLE_ALL.sql >$YADAMU_LOG_PATH/MSSQL_RECREATE_SCHEMA.log
export MSSQL_SCHEMA=jtest
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_HOME/tests/mssql/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/MSSQL_RECREATE_SCHEMA.log
export MSSQL_SCHEMA=sakila
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_HOME/tests/mssql/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/MSSQL_RECREATE_SCHEMA.log
unset SQLCMDINI
rm setvars.sql
source $YADAMU_HOME/tests/mysql/env/dbConnection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=''" <$YADAMU_HOME/tests/mysql/sql/RECREATE_MSSQL_ALL.sql >$YADAMU_LOG_PATH/MYSQL_RECREATE_SCHEMA.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=''" <$YADAMU_HOME/tests/mysql/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/MYSQL_RECREATE_SCHEMA.log
source $YADAMU_HOME/tests/mariadb/env/dbConnection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @SCHEMA='jtest'" <$YADAMU_HOME/tests/mariadb/sql/RECREATE_SCHEMA.sql >$YADAMU_LOG_PATH/MARIADB_RECREATE_SCHEMA.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @SCHEMA='sakila'" <$YADAMU_HOME/tests/mariadb/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/MARIADB_RECREATE_SCHEMA.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=''" <$YADAMU_HOME/tests/mysql/sql/RECREATE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/MARIADB_RECREATE_SCHEMA.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=''" <$YADAMU_HOME/tests/mysql/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/MARIADB_RECREATE_SCHEMA.log
source $YADAMU_HOME/tests/postgres/env/dbConnection.sh
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vSCHEMA=jtest -vID="" -f $YADAMU_HOME/tests/postgres/sql/RECREATE_SCHEMA.sql > $YADAMU_LOG_PATH/POSTGRES_RECREATE_SCHEMA.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vSCHEMA=sakila -vID="" -f $YADAMU_HOME/tests/postgres/sql/RECREATE_SCHEMA.sql >> $YADAMU_LOG_PATH/POSTGRES_RECREATE_SCHEMA.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vID="" -f $YADAMU_HOME/tests/postgres/sql/RECREATE_MSSQL_ALL.sql >>$YADAMU_LOG_PATH/POSTGRES_RECREATE_SCHEMA.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -vID="" -f $YADAMU_HOME/tests/postgres/sql/RECREATE_ORACLE_ALL.sql >>$YADAMU_LOG_PATH/POSTGRES_RECREATE_SCHEMA.log