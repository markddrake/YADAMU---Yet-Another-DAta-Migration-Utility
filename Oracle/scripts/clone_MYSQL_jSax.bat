@SET TNS=ORCL18c
@set MDIR=..\JSON\MSSQL
@SET DIR=JSON\%TNS%\MYSQL
@SET SCHEMA=SAKILA
@SET FILENAME=sakila
@SET ID=1
mkdir %DIR%
sqlplus system/oracle@%TNS% @SQL/COMPILE_ALL
sqlplus system/oracle@%TNS% @TESTS/RECREATE_SCHEMA.sql %SCHEMA% %ID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%MDIR%\%FILENAME%.json toUser=%SCHEMA%%ID%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
@SET ID=2
sqlplus system/oracle@%TNS% @TESTS/RECREATE_SCHEMA.sql %SCHEMA% %ID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\%FILENAME%1.json touser=%SCHEMA%%ID%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\%FILENAME%%ID%.json owner=%SCHEMA%%ID%
dir %DIR%\*1.json
dir %DIR%\*2.json
