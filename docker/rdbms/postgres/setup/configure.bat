set STAGE=c:\stage
cd %STAGE%
powershell -command (Get-Service postgresql-x64-14).waitForStatus("""Running""","""00:03:00""");
mkdir %STAGE%\log
set DB_USER=postgres
set PGPASSWORD=oracle
set DB_DBNAME=yadamu
psql -U %DB_USER% -f setup/configure.sql > log/configure.log
psql -U %DB_USER% -d %DB_DBNAME% -a -f sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log
psql -U %DB_USER% -d %DB_DBNAME% -a -f sql/YADAMU_TEST.sql > log/YADAMU_TEST.log
psql -U %DB_USER% -d %DB_DBNAME% -a -f testdata/dataTypeTesting.sql > log/dataTypeTesting.log