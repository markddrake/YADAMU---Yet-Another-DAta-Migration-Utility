export PATH=$PATH:/opt/mssql-tools/bin:/opt/mssql-tools18/bin
export MSSQL_VERSION=$1
which sqlcmd
export STAGE=/var/opt/mssql/stage
mkdir -p $STAGE/log
cd $STAGE
export DB_USER=sa
export DB_PWD=oracle#1
export DB_HOST=localhost
# Configure Max Memory 12G
export MSSQL_MEMORY=12288
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/configure.sql > log/configure.log 2>&1
# Install YADAMU_IMPORT
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log 2>&1
cat log/YADAMU_IMPORT.log
# Install YADAMU_COMPARE
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i sql/YADAMU_COMPARE.sql   > log/YADAMU_COMPARE.log 2>&1
cat log/YADAMU_COMPARE.log
# Install Northwind
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i testdata/Northwind/instnwnd.sql > log/instnwnd.log 2>&1
# Install AdventureWorks
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/AdventureWorks.sql -v MSSQL_VERSION=$MSSQL_VERSION > log/AdventureWorks.log 2>&1
cat log/AdventureWorks.log
# Install AdventureWorks Data Warehouse
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/AdventureWorksDW.sql -v MSSQL_VERSION=$MSSQL_VERSION  > log/AdventureWorksDW.log 2>&1
cat log/AdventureWorksDW.log
# Install Wide World Importers
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/WorldWideImporters.sql -v MSSQL_VERSION=$MSSQL_VERSION > log/WorldWideImporters.log 2>&1
cat log/WorldWideImporters.log
# Install Wide World Importers Data Warehouse
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/WorldWideImportersDW.sql -v MSSQL_VERSION=$MSSQL_VERSION > log/WorldWideImportersDW.log 2>&1
cat log/WorldWideImportersDW.log 
