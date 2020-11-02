# If running manually, run from the Yadamu 'home' folder.
if [ -z ${YADAMU_HOME+x} ]; then export YADAMU_HOME=`pwd`; fi
export YADAMU_INSTALL=$YADAMU_HOME/src/install
export YADAMU_LOG_PATH=$YADAMU_HOME/log
if [ -e $YADAMU_LOG_PATH ]; then rm -rf $YADAMU_LOG_PATH; fi
mkdir $YADAMU_LOG_PATH
export YADAMU_LOG_PATH=$YADAMU_LOG_PATH/install
mkdir $YADAMU_LOG_PATH
export YADAMU_DB=oracle19c
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_INSTALL/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH/$YADAMU_DB
export YADAMU_DB=oracle18c
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_INSTALL/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH/$YADAMU_DB
export YADAMU_DB=oracle12c
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_INSTALL/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH/$YADAMU_DB
export YADAMU_DB=oracle11g
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @$YADAMU_INSTALL/oracle/sql/COMPILE_ALL.sql $YADAMU_LOG_PATH/$YADAMU_DB
export YADAMU_DB=mssql
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i$YADAMU_INSTALL/mssql/sql/YADAMU_IMPORT.sql >$YADAMU_LOG_PATH/$YADAMU_DB/YADAMU_IMPORT.log
export YADAMU_DB=mysql
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <$YADAMU_INSTALL/mysql/sql/YADAMU_IMPORT.sql  >$YADAMU_LOG_PATH/$YADAMU_DB/YADAMU_IMPORT.log
export YADAMU_DB=postgres
mkdir $YADAMU_LOG_PATH/$YADAMU_DB
source $YADAMU_INSTALL/$YADAMU_DB/env/dbConnection.sh
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f $YADAMU_INSTALL/postgres/sql/YADAMU_IMPORT.sql >$YADAMU_LOG_PATH/$YADAMU_DB/YADAMU_IMPORT.log
