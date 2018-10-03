psql -U postgres -h 192.168.1.250 -f TESTS/RECREATE_SCHEMAS.sql -a -v ID=2
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\HR.json toUser=HR2 logfile=logs\import\HR2.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\SH.json toUser=SH2 logfile=logs\import\SH2.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\OE.json toUser=OE2 logfile=logs\import\OE2.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\PM.json toUser=PM2 logfile=logs\import\PM2.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\IX.json toUser=IX2 logfile=logs\import\IX2.log
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle File=JSON\BI.json toUser=BI2 logfile=logs\import\BI2.log