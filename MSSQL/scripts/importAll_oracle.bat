sqlcmd -Usa -Poracle -S192.168.1.250 -d clone -I -e -i TESTS\RECREATE_SCHEMAS.sql -v ID=1
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"HR1\" --FILE=..\JSON\oracle\18c\HR.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"SH1\" --FILE=..\JSON\oracle\18c\SH.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"OE1\" --FILE=..\JSON\oracle\18c\OE.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"PM1\" --FILE=..\JSON\oracle\18c\PM.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"IX1\" --FILE=..\JSON\oracle\18c\IX.json
node node\import --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=clone --TOUSER=\"BI1\" --FILE=..\JSON\oracle\18c\BI.json