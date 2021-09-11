cd /var/lib/postgresql/data
psql -Upostgres  -f setup/configure.sql
export DB_USER=postgres
export DB_PWD=oracle
export DB_HOST=localhost
export DB_DBNAME=yadamu
mkdir -p sql/log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f sql/YADAMU_IMPORT.sql > sql/log/YADAMU_IMPORT.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f sql/YADAMU_TEST.sql > sql/log/YADAMU_TEST.log
psql -U $DB_USER -d $DB_DBNAME -h $DB_HOST -a -f testdata/dataTypeTesting.sql > sql/log/dataTypeTesting.log
rm -rf sql
rm -rf setup