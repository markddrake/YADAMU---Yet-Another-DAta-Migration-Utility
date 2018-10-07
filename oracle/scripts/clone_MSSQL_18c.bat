mkdir JSON\ORCL18c\MSSQL
sqlplus system/oracle@ORCL18c @SQL/COMPILE_ALL
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMA.sql ADVWRK 1
call scripts\import_master_MSSQL.bat ORCL18c ADVWRK1
node node\export userid=SYSTEM/oracle@ORCL18c File=JSON\ORCL18c\MSSQL\AdventureWorks1.json owner=ADVWRK1
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMA.sql ADVWRK 2
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\ORCL18c\MSSQL\AdventureWorks1.json touser=ADVWRK2
node node\export userid=SYSTEM/oracle@ORCL18c File=JSON\ORCL18c\MSSQL\AdventureWorks2.json owner=ADVWRK2
dir JSON\ORCL18c\MSSQL\*1*.json
dir JSON\ORCL18c\MSSQL\*2*.json
