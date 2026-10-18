@if defined POSTGRES_USER (set DB_USER=%POSTGRES_USER%) else (set DB_USER=postgres)
@if defined POSTGRES_PWD (set DB_PWD=%POSTGRES_PWD%) else (set DB_PWD=oracle)
@if defined POSTGRES_HOST (set DB_HOST=%POSTGRES_HOST%) else (set DB_HOST=localhost)
@if defined POSTGRES_PORT (set DB_PORT=%POSTGRES_PORT%) else (set DB_PORT=5432)
@if defined POSTGRES_DBNAME (set DB_DBNAME=%POSTGRES_DBNAME%) else (set DB_DBNAME=yadamu)
@set PGPASSWORD=%DB_PWD%