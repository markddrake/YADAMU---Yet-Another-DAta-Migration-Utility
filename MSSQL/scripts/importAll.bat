sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMAS.sql -v ID=2
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"HR2\" --FILE=c:JSON\\HR.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"SH2\" --FILE=c:JSON\\SH.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"OE2\" --FILE=c:JSON\\OE.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"PM2\" --FILE=c:JSON\\PM.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"IX2\" --FILE=c:JSON\\IX.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"BI2\" --FILE=c:JSON\\BI.json