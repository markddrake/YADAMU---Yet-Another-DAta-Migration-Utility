echo "Using Docker"
# If a value is not set here a default will be assigned.
# MariaDB Connection Information
unset MARIADB_USER
unset MARIADB_PWD
export MARIADB_HOST=MARIA10-01
unset MARIADB_PORT
unset MARIADB_DBNAME
# Generic SQL Server Connection Information
unset MSSQL_USER
unset MSSQL_PWD
export MSSQL_HOST=MSSQL19-01
unset MSSQL_PORT
unset MSSQL_DBNAME
# SQL Server 2017 Connection Information
unset MSSQL17_USER
unset MSSQL17_PWD
export MSSQL17_HOST=MSSQL17-01
unset MSSQL17_PORT
unset MSSQL17_DBNAME
# SQL Server 2019 Connection Information
unset MSSQL19_USER
unset MSSQL19_PWD
export MSSQL19_HOST=MSSQL19-01
unset MSSQL19_PORT
unset MSSQL19_DBNAME
# MySQL Connection Information
unset MYSQL_USER
unset MYSQL_PWD
export MYSQL_HOST=MYSQL80-01
unset MYSQL_PORT
unset MYSQL_DBNAME
# Oracle 19c Connection Information
export ORACLE19C=ORA1903
unset ORACLE19C_USER
unset ORACLE19C_PWD
# Oracle 18c Connection Information
export ORACLE18C=ORA1803
unset ORACLE18C_USER
unset ORACLE18C_PWD
# Oracle 12c Connection Information
export ORACLE12C=ORA1220
unset ORACLE12C_USER
unset ORACLE12C_PWD
# Oracle 11g Connection Information
export ORACLE11G=ORA1120
unset ORACLE11G_USER
unset ORACLE11G_PWD
# Postgres Connection Information
unset POSTGRES_USER
unset POSTGRES_PWD
export POSTGRES_HOST=PGSQL13-01
unset POSTGRES_PORT
unset POSTGRES_DBNAME
# Relative location of export files to be used for testing
export YADAMU_ORACLE_PATH=oracle19c
export YADAMU_MSSQL_PATH=mssql19
export YADAMU_MYSQL_PATH=mysql
export YADAMU_TEST_FOLDER=cmdLine