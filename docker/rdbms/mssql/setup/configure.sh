export STAGE=/var/opt/mssql/stage
mkdir -p $STAGE/log
cd $STAGE
export DB_USER=sa
export DB_PWD=oracle#1
export DB_HOST=localhost
export MSSQL_MEMORY=16384
# Configure Max Memory 16G
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/configure.sql > log/configure.log
# Install Northwind
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i testdata/instnwnd2017.sql > log/instnwnd2017.log
# Install AdventureWorks
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/AdventureWorks.sql > log/AdventureWorks.log
cat log/AdventureWorks.log
# Install AdventureWorks Data Warehouse
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/AdventureWorksDW.sql > log/AdventureWorksDW.log
cat log/AdventureWorksDW.log
# Install Wide World Importers
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/WorldWideImporters.sql > log/WorldWideImporters.log
cat log/WorldWideImporters.log
# Install Wide World Importers Data Warehouse
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/WorldWideImportersDW.sql> log/WorldWideImportersDW.log 
cat log/WorldWideImportersDW.log 
# Install YADAMU_IMPORT
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
cat log/YADAMU_IMPORT.log
# Install YADAMU_TEST
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i sql/YADAMU_TEST.sql   > log/YADAMU_TEST.log
