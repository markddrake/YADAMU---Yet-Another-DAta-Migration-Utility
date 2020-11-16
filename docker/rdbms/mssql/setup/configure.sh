cd /var/opt/mssql
export DB_USER=sa
export DB_PWD=oracle#1
export DB_HOST=localhost
mkdir -p sql/log
# Configure Max Memory 16G
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -isql/configure.sql >sql/log/configure.log
# Install Northwind
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i testdata/instnwnd2017.sql
# Install AdvebtureWorks
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/AdventureWorks.sql 
# Install AdvebtureWorksDW
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/AdventureWorksDW.sql 
# Install WorldWideImporters
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i setup/WorldWideImporters.sql 
# Install WorldWideImporters
/opt/mssql-tools/bin/sqlcmd -Usa -Poracle#1 -i setup/WorldWideImportersDW.sql 
# Install YADAMU_IMPORT
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -isql/YADAMU_IMPORT.sql >sql/log/YADAMU_IMPORT.log
# Install YADAMU_TEST
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -isql/YADAMU_TEST.sql   >sql/logYADAMU_TEST.log
rm -rf sql
rm -rf setup