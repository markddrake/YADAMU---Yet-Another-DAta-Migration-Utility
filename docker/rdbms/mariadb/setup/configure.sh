# Temp Install MySQLShell to configure database. Changes will be lost when container restarts but that's OK
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y wget lsb-release gnupg

# Download the .deb file
wget http://repo.mysql.com/apt/ubuntu/pool/mysql-8.0/m/mysql-shell/mysql-shell_8.0.44-1ubuntu24.04_amd64.deb

# Install dependencies first
apt-get update
apt-get install -y libantlr4-runtime4.10 libtinyxml2-10 libprotobuf32t64

# Install the downloaded .deb
apt-get install -y ./mysql-shell_8.0.44-1ubuntu24.04_amd64.deb

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