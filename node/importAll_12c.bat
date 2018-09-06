sqlplus system/oracle@ORCL12c @../TESTS/RECREATE_SCHEMAS.sql
node jsonImport userid=SYSTEM/oracle@ORCL12c File=exp\HR.12c.json toUser=HR1 logfile=log\HR.import.12c.log
node jsonImport userid=SYSTEM/oracle@ORCL12c File=exp\SH.12c.json toUser=SH1 logfile=log\SH.import.12c.log
node jsonImport userid=SYSTEM/oracle@ORCL12c File=exp\OE.12c.json toUser=OE1 logfile=log\OE.import.12c.log
node jsonImport userid=SYSTEM/oracle@ORCL12c File=exp\PM.12c.json toUser=PM1 logfile=log\PM.import.12c.log
node jsonImport userid=SYSTEM/oracle@ORCL12c File=exp\IX.12c.json toUser=IX1 logfile=log\IX.import.12c.log
node jsonImport userid=SYSTEM/oracle@ORCL12c File=exp\BI.12c.json toUser=BI1 logfile=log\BI.import.12c.log