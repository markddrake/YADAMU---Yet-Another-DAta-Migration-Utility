sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMA MYSQL 1
node node\import userid=SYSTEM/oracle@ORCL18c  TOUSER=\"MYSQL1\" FILE=..\JSON\MYSQL\sakila.json
