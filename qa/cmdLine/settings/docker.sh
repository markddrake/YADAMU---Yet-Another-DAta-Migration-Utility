echo "Using Docker"
# If a value is not set here a default will be assigned.
# MariaDB Connection Information
unset MARIADB_USER
unset MARIADB_PWD
export MARIADB_HOST=MARIA-DB1
unset MARIADB_PORT
unset MARIADB_DBNAME
# MySQL Connection Information
unset MYSQL_USER
unset MYSQL_PWD
export MYSQL_HOST=MYSQL-DB1
unset MYSQL_PORT
unset MYSQL_DBNAME
# Postgres Connection Information
unset POSTGRES_USER
unset POSTGRES_PWD
export POSTGRES_HOST=POSTGRES-DB1
unset POSTGRES_PORT
unset POSTGRES_DBNAME
# Generic SQL Server Connection Information #1 
unset MSSQL_01_USER
unset MSSQL_01_PWD
export MSSQL_01_HOST=MSSQL-DB1
unset MSSQL_01_PORT
unset MSSQL_01_DBNAME
# Generic SQL Server Connection Information #2
unset MSSQL_02_USER
unset MSSQL_02_PWD
export MSSQL_02_HOST=MSSQL-DB1
unset MSSQL_02_PORT
unset MSSQL_02_DBNAME
# SQL Server 2017 Connection Information
unset MSSQL17_USER
unset MSSQL17_PWD
export MSSQL17_HOST=MSSQL-DB2
unset MSSQL17_PORT
unset MSSQL17_DBNAME
# SQL Server 2019 Connection Information
unset MSSQL19_USER
unset MSSQL19_PWD
export MSSQL19_HOST=MSSQL19-01
unset MSSQL19_PORT
unset MSSQL19_DBNAME
# SQL Server 2022 Connection Information
unset MSSQL22_USER
unset MSSQL22_PWD
export MSSQL22_HOST=MSSQL22-01
unset MSSQL22_PORT
unset MSSQL22_DBNAME
# Oracle #1 Connection Information
export ORACLE_01=ORACL01
unset ORACLE_01_USER
unset ORACLE_01_PWD
# Oracle #1 Connection Information
export ORACLE_02=ORACL02
unset ORACLE_02_USER
unset ORACLE_02_PWD
# Oracle #1 Connection Information
export ORACLE_03=ORACL03
unset ORACLE_03_USER
unset ORACLE_03_PWD
# Oracle #1 Connection Information
export ORACLE_04=ORACL04
unset ORACLE_04_USER
unset ORACLE_04_PWD
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
# Relative location of export files to be used for testing
export YADAMU_ORACLE_PATH=oracle#1
export YADAMU_MSSQL_PATH=mssql#1
export YADAMU_MYSQL_PATH=mysql
export YADAMU_TEST_FOLDER=cmdLine