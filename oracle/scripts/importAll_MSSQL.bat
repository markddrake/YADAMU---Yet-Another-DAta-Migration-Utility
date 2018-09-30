sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMAS.sql 3
node node\import userid=SYSTEM/oracle@ORCL18c File=..\\JSON\\MSSQL\\HR.json toUser=HR3 
node node\import userid=SYSTEM/oracle@ORCL18c File=..\\JSON\\MSSQL\\SH.json toUser=SH3 
node node\import userid=SYSTEM/oracle@ORCL18c File=..\\JSON\\MSSQL\\OE.json toUser=OE3 
node node\import userid=SYSTEM/oracle@ORCL18c File=..\\JSON\\MSSQL\\PM.json toUser=PM3 
node node\import userid=SYSTEM/oracle@ORCL18c File=..\\JSON\\MSSQL\\IX.json toUser=IX3 
node node\import userid=SYSTEM/oracle@ORCL18c File=..\\JSON\\MSSQL\\BI.json toUser=BI3 