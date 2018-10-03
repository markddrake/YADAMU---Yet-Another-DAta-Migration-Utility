psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMAS.sql -a -v ID=1
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\HR.json toUser=HR1 logfile=logs\oracle\HR1.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\SH.json toUser=SH1 logfile=logs\oracle\SH1.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\OE.json toUser=OE1 logfile=logs\oracle\OE1.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\PM.json toUser=PM1 logfile=logs\oracle\PM1.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\IX.json toUser=IX1 logfile=logs\oracle\IX1.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\oracle\BI.json toUser=BI1 logfile=logs\oracle\BI1.log