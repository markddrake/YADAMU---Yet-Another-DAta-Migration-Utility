sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMAS.sql 1
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\HR_DATA_ONLY.json toUser=HR1
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\SH_DATA_ONLY.json toUser=SH1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\OE_DATA_ONLY.json toUser=OE1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\PM_DATA_ONLY.json toUser=PM1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\IX_DATA_ONLY.json toUser=IX1 
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\BI_DATA_ONLY.json toUser=BI1 