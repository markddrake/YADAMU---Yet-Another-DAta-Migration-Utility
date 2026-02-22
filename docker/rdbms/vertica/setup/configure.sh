export STAGE=/data/vertica/stage
cd $STAGE
mkdir -p log
export DB_USER=dbadmin
export DB_PWD=oracle
export DB_DBNAME=VMart
$VERTICA_OPT_BIN/vsql -U$DB_USER -w$DB_PWD -d $DB_DBNAME -a -f sql/YADAMU_IMPORT.sql -o log/YADAMU_IMPORT.log
$VERTICA_OPT_BIN/vsql -U$DB_USER -w$DB_PWD -d $DB_DBNAME -a -f sql/YADAMU_COMPARE.sql -o log/YADAMU_COMPARE.log
