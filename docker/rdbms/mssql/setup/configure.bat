set STAGE=c:\STAGE
cd %STAGE%
powershell -command New-Item -Force -ItemType directory -Path ($env:STAGE + """\log""")
powershell -command (Get-Service """MSSQLSERVER""").waitForStatus("""Running""","""00:03:00""");
set DB_USER=sa
set DB_PWD=oracle#1
set DB_HOST=localhost
set MSSQL_MEMORY=8192
REM Configure Max Memory
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i setup/configure.sql > log/configure.log
REM Install Northwind
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i testdata/instnwnd2017.sql > log/instnwnd2017.log
REM Install AdvebtureWorks
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i setup/AdventureWorks.sql > log/AdventureWorks.log 
type log\AdventureWorks.log 
REM Install AdvebtureWorksDW
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i setup/AdventureWorksDW.sql > log/AdventureWorksDW.log 
type log\AdventureWorksDW.log 
REM Install Wide World Importers
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i setup/WorldWideImporters.sql > log/WorldWideImporters.log 
type log\WorldWideImporters.log 
REM Install WorldWideImporters
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i setup/WorldWideImportersDW.sql > log/WorldWideImportersDW.log 
type log\WorldWideImportersDW.log 
REM Install Wide World Importers Data Warehouse
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
type log\YADAMU_IMPORT.log 
REM Install YADAMU_COMPARE
sqlcmd -U%DB_USER% -P%DB_PWD% -dmaster -I -e -i sql/YADAMU_COMPARE.sql   > log/YADAMU_COMPARE.log
