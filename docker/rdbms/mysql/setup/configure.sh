# Temp Install MySQLShell to configure database. Changes will be lost when container restarts but that's OK
# apt-get update
# apt-get install -y curl
# apt-get install -y lsb-release
# apt-get install -y wget
# curl https://repo.mysql.com//mysql-apt-config_0.8.22-1_all.deb >  mysql-apt-config_0.8.22-1_all.deb
# export DEBIAN_FRONTEND="noninteractive";
# dpkg -i mysql-apt-config*.deb
# apt-get update  --allow-unauthenticated  
# apt-get install -y mysql-shell
export STAGE=/var/lib/mysql/stage
cd $STAGE
mkdir -p log
export DB_USER=root
export DB_PWD=oracle
export DB_DBNAME=sys
mysql   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f < setup/configure.sql                > log/configure.log 2>&1
mysql   -u$DB_USER -p$DB_PWD -Dsakila     -v -f < testdata/sakila/sakila-schema.sql >> log/configure.log 2>&1
mysql   -u$DB_USER -p$DB_PWD -Dsakila     -v -f < testdata/sakila/sakila-data.sql   >> log/configure.log 2>&1
mysql   -u$DB_USER -p$DB_PWD -Djtest      -v -f <  testdata/jtest.audit.sql         >> log/configure.log 2>&1
mysqlsh -u$DB_USER -p$DB_PWD -D$DB_DBNAME --js --interactive --file=setup/YADAMU_INSTALL.js > log/install.log 2>&1
cat log/install.log
mysql   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f < sql/YADAMU_COMPARE.sql > log/YADAMU_COMPARE.log 2>&1
 