node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\HR.json toUser=HR2 logfile=logs\import\HR.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\SH.json toUser=SH2 logfile=logs\import\SH.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\OE.json toUser=OE2 logfile=logs\import\OE.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\PM.json toUser=PM2 logfile=logs\import\PM.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\IX.json toUser=IX2 logfile=logs\import\IX.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\BI.json toUser=BI2 logfile=logs\import\BI.log