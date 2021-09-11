cd /var/lib/mysql
apt-get update
apt-get install -y curl
apt-get install -y lsb-release
apt-get install -y wget
curl https://repo.mysql.com//mysql-apt-config_0.8.18-1_all.deb >  mysql-apt-config_0.8.18-1_all.deb
export DEBIAN_FRONTEND="noninteractive";
dpkg -i mysql-apt-config*.deb
apt-get update
apt-get install -y mysql-shell
mysql -uroot -poracle <setup/configure.sql
export DB_USER=root
export DB_PWD=oracle
export DB_HOST=localhost
export DB_DBNAME=sys
export DB_PORT=3306
mkdir -p sql/log
mysqlsh  -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT  --js --interactive --file=setup/YADAMU_INSTALL.js
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <sql/YADAMU_TEST.sql >sql/log/YADAMU_TEST.log
rm -rf sql
rm -rf setup
rm -rf testdata