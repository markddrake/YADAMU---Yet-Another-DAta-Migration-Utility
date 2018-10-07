node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone TOUSER=\"HR%~1\" FILE=JSON\Oracle\HR%~2.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone TOUSER=\"SH%~1\" FILE=JSON\Oracle\SH%~2.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone TOUSER=\"OE%~1\" FILE=JSON\Oracle\OE%~2.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone TOUSER=\"PM%~1\" FILE=JSON\Oracle\PM%~2.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone TOUSER=\"IX%~1\" FILE=JSON\Oracle\IX%~2.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone TOUSER=\"BI%~1\" FILE=JSON\Oracle\BI%~2.json
