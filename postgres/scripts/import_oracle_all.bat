node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"HR%~1\" FILE=JSON\Oracle\18c\HR%~2.json
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"SH%~1\" FILE=JSON\Oracle\18c\SH%~2.json
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"OE%~1\" FILE=JSON\Oracle\18c\OE%~2.json
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"PM%~1\" FILE=JSON\Oracle\18c\PM%~2.json
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"IX%~1\" FILE=JSON\Oracle\18c\IX%~2.json
node node\import --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  TOUSER=\"BI%~1\" FILE=JSON\Oracle\18c\BI%~2.json
