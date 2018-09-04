sqlplus system/oracle@ORCL @../TESTS/RECREATE_SCHEMAS.sql
node jsonImport userid=SYSTEM/oracle@ORCL File=exp\HR2.json  toUser=HR2 logfile=log\HR.import.log
node jsonImport userid=SYSTEM/oracle@ORCL File=exp\SH2.json  toUser=SH2 logfile=log\SH.import.log
node jsonImport userid=SYSTEM/oracle@ORCL File=exp\OE2.json  toUser=OE2 logfile=log\OE.import.log
node jsonImport userid=SYSTEM/oracle@ORCL File=exp\PM2.json  toUser=PM2 logfile=log\PM.import.log
node jsonImport userid=SYSTEM/oracle@ORCL File=exp\IX2.json  toUser=IX2 logfile=log\IX.import.log
node jsonImport userid=SYSTEM/oracle@ORCL File=exp\BI2.json  toUser=BI2 logfile=log\BI.import.log