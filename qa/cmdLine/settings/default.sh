echo "Using default unsetings"
# If a value is not unset here a default will be assigned.
# MariaDB Connection Information
unset MARIADB_USER
unset MARIADB_PWD
unset MARIADB_HOST
unset MARIADB_PORT
unset MARIADB_DBNAME
# Generic SQL Server Connection Information
unset MSSQL_USER
unset MSSQL_PWD
unset MSSQL_HOST
unset MSSQL_PORT
unset MSSQL_DBNAME
# SQL Server 2017 Connection Information
unset MSSQL17_USER
unset MSSQL17_PWD
unset MSSQL17_HOST
unset MSSQL17_PORT
unset MSSQL17_DBNAME
# SQL Server 2019 Connection Information
unset MSSQL19_USER
unset MSSQL19_PWD
unset MSSQL19_HOST
unset MSSQL19_PORT
unset MSSQL19_DBNAME
# MySQL Connection Information
unset MYSQL_USER
unset MYSQL_PWD
unset MYSQL_HOST
unset MYSQL_PORT
unset MYSQL_DBNAME
# Oracle 19c Connection Information
unset ORACLE19C
unset ORACLE19C_USER
unset ORACLE19C_PWD
# Oracle 18c Connection Information
unset ORACLE18C
unset ORACLE18C_USER
unset ORACLE18C_PWD
# Oracle 12c Connection Information
unset ORACLE12C
unset ORACLE12C_USER
unset ORACLE12C_PWD
# Oracle 11g Connection Information
unset ORACLE11G
unset ORACLE11G_USER
unset ORACLE11G_PWD
# Postgres Connection Information
unset POSTGRES_USER
unset POSTGRES_PWD
unset POSTGRES_HOST
unset POSTGRES_PORT
unset POSTGRES_DBNAME
# Relative location of export files to be used for testing
export YADAMU_ORACLE_PATH=oracle19c
export YADAMU_MSSQL_PATH=mssql19
export YADAMU_MYSQL_PATH=mysql
export YADAMU_TEST_FOLDER=cmdLine
@echo on