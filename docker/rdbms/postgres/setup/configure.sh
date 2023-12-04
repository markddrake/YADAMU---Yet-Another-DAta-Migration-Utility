export STAGE=/var/lib/postgresql/stage
cd $STAGE
mkdir -p $STAGE/log
export DB_USER=postgres
export PGPASSWORD=oracle
export DB_DBNAME=yadamu
psql -U $DB_USER -a -e -f setup/configure.sql > log/configure.log
psql -U $DB_USER -d $DB_DBNAME -a -e -f sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
psql -U $DB_USER -d $DB_DBNAME -a -e -f sql/YADAMU_COMPARE.sql > log/YADAMU_COMPARE.log
psql -U $DB_USER -d $DB_DBNAME -a -e -f testdata/dataTypeTesting.sql > log/dataTypeTesting.log