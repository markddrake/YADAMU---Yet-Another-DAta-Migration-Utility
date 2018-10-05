sqlplus system/oracle@ORCL18c @SQL/COMPILE_ALL
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMAS.sql 1
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\HR_DDL_DATA.json toUser=HR1
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\SH_DDL_DATA.json toUser=SH1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\OE_DDL_DATA.json toUser=OE1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\PM_DDL_DATA.json toUser=PM1
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\IX_DDL_DATA.json toUser=IX1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\BI_DDL_DATA.json toUser=BI1