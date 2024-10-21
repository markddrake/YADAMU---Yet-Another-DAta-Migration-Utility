export PATH=$PATH:/opt/mssql-tools/bin:/opt/mssql-tools18/bin
which sqlcmd
export STAGE=/var/opt/mssql/stage
mkdir -p $STAGE/log
cd $STAGE
export DB_USER=sa
export DB_PWD=oracle#1
export DB_HOST=localhost
export MSSQL_MEMORY=16384
# Configure Max Memory 16G
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/configure.sql > log/configure.log
# Install Northwind
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i testdata/instnwnd2017.sql > log/instnwnd2017.log
# Install AdventureWorks
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/AdventureWorks.sql > log/AdventureWorks.log
cat log/AdventureWorks.log
# Install AdventureWorks Data Warehouse
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/AdventureWorksDW.sql > log/AdventureWorksDW.log
cat log/AdventureWorksDW.log
# Install Wide World Importers
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/WorldWideImporters.sql > log/WorldWideImporters.log
cat log/WorldWideImporters.log
# Install Wide World Importers Data Warehouse
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/WorldWideImportersDW.sql> log/WorldWideImportersDW.log 
cat log/WorldWideImportersDW.log 
# Install YADAMU_IMPORT
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
cat log/YADAMU_IMPORT.log
# Install YADAMU_COMPARE
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i sql/YADAMU_COMPARE.sql   > log/YADAMU_COMPARE.log
