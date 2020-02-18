cd /var/lib/mysql
mysql -uroot -poracle <setup/configure.sql
export DB_USER=root
export DB_PWD=oracle
export DB_HOST=localhost
export DB_DBNAME=sys
export DB_PORT=3306
mkdir -p sql/log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <sql/YADAMU_IMPORT.sql  >sql/log/YADAMU_IMPORT.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <sql/YADAMU_TEST.sql >sql/log/YADAMU_TEST.log
rm -rf sql
rm -rf setup
rm -rf testdata