# Temp Install MySQLShell to configure database. Changes will be lost when container restarts but that's OK
# Uses the self-contained glibc tarball rather than a distro .deb, so it does not depend on the Ubuntu release the mariadb image is built on
export DEBIAN_FRONTEND=noninteractive
export MYSQLSH_VERSION=26.7.0
apt-get update
apt-get install -y curl ca-certificates
curl -sL https://dev.mysql.com/get/Downloads/MySQL-Shell/mysql-shell-${MYSQLSH_VERSION}-linux-glibc2.28-x86-64bit.tar.gz | tar xz -C /opt
ln -sf /opt/mysql-shell-${MYSQLSH_VERSION}-linux-glibc2.28-x86-64bit/bin/mysqlsh /usr/local/bin/mysqlsh

export STAGE=/var/lib/mysql/stage
cd $STAGE
mkdir -p log
export DB_USER=root
export DB_PWD=oracle
export DB_DBNAME=mysql
mariadb   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f < setup/configure.sql                > log/configure.log 
mariadb   -u$DB_USER -p$DB_PWD -Dsakila     -v -f < testdata/sakila/sakila-schema.sql >> log/configure.log 2>&1
mariadb   -u$DB_USER -p$DB_PWD -Dsakila     -v -f < testdata/sakila/sakila-data.sql   >> log/configure.log 2>&1
mariadb   -u$DB_USER -p$DB_PWD -Djtest      -v -f <  testdata/jtest.audit.sql         >> log/configure.log 2>&1
mysqlsh   -u$DB_USER -p$DB_PWD -D$DB_DBNAME --js --interactive --file=setup/YADAMU_INSTALL.js  > log/install.log 2>&1
cat log/install.log
mariadb   -u$DB_USER -p$DB_PWD -D$DB_DBNAME -v -f < sql/YADAMU_COMPARE.sql > log/YADAMU_COMPARE.log  2>&1