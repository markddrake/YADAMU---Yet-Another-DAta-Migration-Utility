sh $YADAMU_HOME/tests/oracle/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_HOME/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH
. $YADAMU_HOME/tests/mssql/env/dbConnection.sh
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_HOME/mssql/sql/JSON_IMPORT.sql > $YADAMU_LOG_PATH/JSON_IMPORT_MSSQL.log
. $YADAMU_HOME/tests/mysql/env/dbConnection.sh
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/mysql/sql/JSON_IMPORT.sql >$YADAMU_LOG_PATH/JSON_IMPORT_MYSQL.log
. $YADAMU_HOME/tests/mariadb/env/dbConnection.sh
mysql.exe -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_HOME/mariadb/sql/SCHEMA_COMPARE.sql >$YADAMU_LOG_PATH/SCHEMA_COMPARE_MARIADB.log
. $YADAMU_HOME/tests/postgres/env/dbConnection.sh
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f $YADAMU_HOME/postgres/sql/JSON_IMPORT.sql > $YADAMU_LOG_PATH/JSON_IMPORT_POSTGRES.log