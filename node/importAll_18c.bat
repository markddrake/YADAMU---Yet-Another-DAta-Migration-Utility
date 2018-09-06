sqlplus system/oracle@ORCL18c @../TESTS/RECREATE_SCHEMAS.sql
node jsonImport userid=SYSTEM/oracle@ORCL18c File=exp\HR.18c.json toUser=HR1 logfile=log\HR.import.18c.log
node jsonImport userid=SYSTEM/oracle@ORCL18c File=exp\SH.18c.json toUser=SH1 logfile=log\SH.import.18c.log
node jsonImport userid=SYSTEM/oracle@ORCL18c File=exp\OE.18c.json toUser=OE1 logfile=log\OE.import.18c.log
node jsonImport userid=SYSTEM/oracle@ORCL18c File=exp\PM.18c.json toUser=PM1 logfile=log\PM.import.18c.log
node jsonImport userid=SYSTEM/oracle@ORCL18c File=exp\IX.18c.json toUser=IX1 logfile=log\IX.import.18c.log
node jsonImport userid=SYSTEM/oracle@ORCL18c File=exp\BI.18c.json toUser=BI1 logfile=log\BI.import.18c.log