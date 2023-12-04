set STAGE=c:\ProgramData\MySQL\%MYSQL_VERSION%\Stage
cd %STAGE%
mkdir log
set DB_USER=root
set DB_PWD=oracle
set DB_DBNAME=sys
mysql   -u%DB_USER% -p%DB_PWD% -D%DB_DBNAME% -v -f < setup\configure.sql > log\configure.log
mysqlsh -u%DB_USER% -p%DB_PWD% -D%DB_DBNAME% --js --interactive --mysql --file=setup\YADAMU_INSTALL.js
mysql   -u%DB_USER% -p%DB_PWD% -D%DB_DBNAME% -v -f < sql\YADAMU_COMPARE.sql > log\YADAMU_COMPARE.log