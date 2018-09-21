node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\HR.json toUser=HR1 logfile=logs\oracle\HR.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\SH.json toUser=SH1 logfile=logs\oracle\SH.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\OE.json toUser=OE1 logfile=logs\oracle\OE.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\PM.json toUser=PM1 logfile=logs\oracle\PM.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\IX.json toUser=IX1 logfile=logs\oracle\IX.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\BI.json toUser=BI1 logfile=logs\oracle\\BI.log