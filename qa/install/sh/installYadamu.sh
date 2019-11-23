source $YADAMU_HOME/tests/oracle19c/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/YADAMU_TEST.sql $YADAMU_LOG_PATH OFF
source $YADAMU_HOME/tests/oracle18c/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/YADAMU_TEST.sql $YADAMU_LOG_PATH OFF
source $YADAMU_HOME/tests/oracle12c/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/YADAMU_TEST.sql $YADAMU_LOG_PATH OFF
source $YADAMU_HOME/tests/oracle11g/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/tests/oracle/sql/YADAMU_TEST.sql $YADAMU_LOG_PATH OFF
source $YADAMU_HOME/tests/mssql/env/dbConnection.sh
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i$YADAMU_HOME/mssql/sql/YADAMU_IMPORT.sql > $YADAMU_LOG_PATH/MSSQL_YADAMU_IMPORT.log
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i$YADAMU_HOME/tests/mssql/sql/YADAMU_TEST.sql > $YADAMU_LOG_PATH/MSSQL_YADAMU_TEST.log
source $YADAMU_HOME/tests/mysql/env/dbConnection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/mysql/sql/YADAMU_IMPORT.sql >$YADAMU_LOG_PATH/MYSQL_YADAMU_IMPORT.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/tests/mysql/sql/YADAMU_TEST.sql >$YADAMU_LOG_PATH/install/MYSQL_YADAMU_TEST.log
source $YADAMU_HOME/tests/mariadb/env/dbConnection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/tests/mariadb/sql/YADAMU_TEST.sql >$YADAMU_LOG_PATH/MARIADB_YADAMU_TEST.log
source $YADAMU_HOME/tests/postgres/env/dbConnection.sh
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f $YADAMU_HOME/postgres/sql/YADAMU_IMPORT.sql > $YADAMU_LOG_PATH/POSTGRES_YADAMU_IMPORT.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f $YADAMU_HOME/tests/postgres/sql/YADAMU_TEST.sql > $YADAMU_LOG_PATH/POSTGRES_YADAMU_TEST.log
