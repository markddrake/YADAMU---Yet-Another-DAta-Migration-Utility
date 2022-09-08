export STAGE=/home/yugabyte/stage
cd $STAGE
mkdir -p $STAGE/log
export DB_USER=postgres
export DB_DBNAME=yadamu
export DB_PORT=5433
ysqlsh -U $DB_USER -h$HOSTNAME -p $DB_PORT -a -e -f setup/configure.sql > log/configure.log
ysqlsh -U $DB_USER -d $DB_DBNAME -h$HOSTNAME -p $DB_PORT -a -e -f sql/YADAMU_PG14_POLYFILL.sql > log/YADAMU_IMPORT.log
ysqlsh -U $DB_USER -d $DB_DBNAME -h$HOSTNAME  -p $DB_PORT -a -e -f sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
ysqlsh -U $DB_USER -d $DB_DBNAME -h$HOSTNAME  -p $DB_PORT -a -e -f sql/YADAMU_TEST.sql > log/YADAMU_TEST.log
