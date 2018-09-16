sqlplus system/oracle@ORCL12c @TESTS/RECREATE_SCHEMAS.sql
node node\import userid=SYSTEM/oracle@ORCL12c File=JSON\12c\HR.json toUser=HR1 logfile=logs\12c\import\HR.log
node node\import userid=SYSTEM/oracle@ORCL12c File=JSON\12c\SH.json toUser=SH1 logfile=logs\12c\import\SH.log
node node\import userid=SYSTEM/oracle@ORCL12c File=JSON\12c\OE.json toUser=OE1 logfile=logs\12c\import\OE.log
node node\import userid=SYSTEM/oracle@ORCL12c File=JSON\12c\PM.json toUser=PM1 logfile=logs\12c\import\PM.log
node node\import userid=SYSTEM/oracle@ORCL12c File=JSON\12c\IX.json toUser=IX1 logfile=logs\12c\import\IX.log
node node\import userid=SYSTEM/oracle@ORCL12c File=JSON\12c\BI.json toUser=BI1 logfile=logs\12c\import\BI.log