# Temp Install MySQLShell to configure database. Changes will be lost when container restarts but that's OK
apt-get update
apt-get install -y curl
apt-get install -y lsb-release
apt-get install -y wget
curl https://repo.mysql.com//mysql-apt-config_0.8.22-1_all.deb >  mysql-apt-config_0.8.22-1_all.deb
export DEBIAN_FRONTEND="noninteractive";
dpkg -i mysql-apt-config*.deb
apt-get update
apt-get install -y mysql-shell
export STAGE=/var/lib/mysql/stage
cd $STAGE
mkdir -p log
export DB_USER=root
export DB_PWD=oracle
export DB_DBNAME=mysql
mysql   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f < setup/configure.sql > log/configure.log
mysqlsh -u$DB_USER -p$DB_PWD -D$DB_DBNAME -hlocalhost -P3306 --js --interactive --file=setup/YADAMU_INSTALL.js
mysql   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f <sql/YADAMU_TEST.sql >log/YADAMU_TEST.log