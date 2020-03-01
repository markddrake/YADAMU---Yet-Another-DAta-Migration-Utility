cd /var/opt/mssql
# Install Northwind
/opt/mssql-tools/bin/sqlcmd -Usa -Poracle#1  -i testdata/instnwnd2017.sql
# Install AdvebtureWorks
/opt/mssql-tools/bin/sqlcmd -Usa -Poracle#1 -i setup/AdventureWorks.sql 
# Install AdvebtureWorksDW
/opt/mssql-tools/bin/sqlcmd -Usa -Poracle#1 -i setup/AdventureWorksDW.sql 
# Install WorldWideImporters
/opt/mssql-tools/bin/sqlcmd -Usa -Poracle#1 -i setup/WorldWideImporters.sql 
# Install WorldWideImporters
/opt/mssql-tools/bin/sqlcmd -Usa -Poracle#1 -i setup/WorldWideImportersDW.sql 
mkdir -p sql/log
export DB_USER=sa
export DB_PWD=oracle#1
export DB_HOST=localhost
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -isql/YADAMU_IMPORT.sql >sql/log/YADAMU_IMPORT.log
/opt/mssql-tools/bin/sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -isql/YADAMU_TEST.sql   >sql/logYADAMU_TEST.log
rm -rf sql
rm -rf setup