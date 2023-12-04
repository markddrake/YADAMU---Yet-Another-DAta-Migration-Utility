set STAGE=c:\stage
cd %STAGE%
set SERVICE_NAME=postgresql-x64-%PGSQL_VERSION%
powershell -command (Get-Service $ENV:SERVICE_NAME).waitForStatus("""Running""","""00:03:00""");
mkdir %STAGE%\log
set DB_USER=postgres
set PGPASSWORD=oracle
set DB_DBNAME=yadamu
psql -U %DB_USER% -f setup/configure.sql > log/configure.log
psql -U %DB_USER% -d %DB_DBNAME% -a -f sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
psql -U %DB_USER% -d %DB_DBNAME% -a -f sql/YADAMU_COMPARE.sql > log/YADAMU_COMPARE.log
psql -U %DB_USER% -d %DB_DBNAME% -a -f testdata/dataTypeTesting.sql > log/dataTypeTesting.log