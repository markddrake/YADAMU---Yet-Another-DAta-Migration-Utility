sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMAS.sql 2
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\HR.json toUser=HR2 logfile=logs\18c\import\HR_NoDDL.log mode=DATA_ONLY
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\SH.json toUser=SH2 logfile=logs\18c\import\SH_NoDDL.log mode=DATA_ONLY
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\OE.json toUser=OE2 logfile=logs\18c\import\OE_NoDDL.log mode=DATA_ONLY
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\PM.json toUser=PM2 logfile=logs\18c\import\PM_NoDDL.log mode=DATA_ONLY
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\IX.json toUser=IX2 logfile=logs\18c\import\IX_NoDDL.log mode=DATA_ONLY
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\BI.json toUser=BI2 logfile=logs\18c\import\BI_NoDDL.log mode=DATA_ONLY