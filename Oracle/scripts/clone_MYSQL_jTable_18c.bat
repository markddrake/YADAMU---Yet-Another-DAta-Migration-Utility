mkdir JSON\ORCL18c\MYSQL
sqlplus system/oracle@ORCL18c @SQL/COMPILE_ALL
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMA.sql sakila 1
node node\jSaxImport userid=SYSTEM/oracle@ORCL18c File=..\JSON\MYSQL\sakila.json toUser=sakila1
node node\export userid=SYSTEM/oracle@ORCL18c File=JSON\ORCL18c\MYSQL\sakila1.json owner=sakila1
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMA.sql sakila 2
node node\jSaxImport userid=SYSTEM/oracle@ORCL18c File=JSON\ORCL18c\MYSQL\sakila1.json touser=sakila2
node node\export userid=SYSTEM/oracle@ORCL18c File=JSON\ORCL18c\MYSQL\sakila2.json owner=sakila2
dir JSON\ORCL18c\MYSQL\*1.json
dir JSON\ORCL18c\MYSQL\*2.json
