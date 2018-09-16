sqlplus system/oracle@ORCL18c @TESTS/RECREATE_SCHEMAS.sql
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\HR.json toUser=HR1 logfile=logs\18c\import\HR.log
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\SH.json toUser=SH1 logfile=logs\18c\import\SH.log
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\OE.json toUser=OE1 logfile=logs\18c\import\OE.log
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\PM.json toUser=PM1 logfile=logs\18c\import\PM.log
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\IX.json toUser=IX1 logfile=logs\18c\import\IX.log
node node\import userid=SYSTEM/oracle@ORCL18c File=JSON\18c\BI.json toUser=BI1 logfile=logs\18c\import\BI.log