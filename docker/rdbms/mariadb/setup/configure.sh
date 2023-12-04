# Temp Install MySQLShell to configure database. Changes will be lost when container restarts but that's OK
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y wget lsb-release gnupg
wget https://repo.mysql.com//mysql-apt-config_0.8.24-1_all.deb
dpkg -i mysql-apt-config_0.8.24-1_all.deb
apt-get update
apt-get install -y mysql-shell
export STAGE=/var/lib/mysql/stage
cd $STAGE
mkdir -p log
export DB_USER=root
export DB_PWD=oracle
export DB_DBNAME=mysql
mariadb   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f < setup/configure.sql > log/configure.log
mysqlsh -u$DB_USER -p$DB_PWD -D$DB_DBNAME -hlocalhost -P3306 --js --interactive --file=setup/YADAMU_INSTALL.js
mariadb   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f <sql/YADAMU_COMPARE.sql >log/YADAMU_COMPARE.log