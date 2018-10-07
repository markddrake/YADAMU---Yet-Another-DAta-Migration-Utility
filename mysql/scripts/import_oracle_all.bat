node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"HR%~1\" FILE=JSON\\Oracle\\HR%~2.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"SH%~1\" FILE=JSON\\Oracle\\SH%~2.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"OE%~1\" FILE=JSON\\Oracle\\OE%~2.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"PM%~1\" FILE=JSON\\Oracle\\PM%~2.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"IX%~1\" FILE=JSON\\Oracle\\IX%~2.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"BI%~1\" FILE=JSON\\Oracle\\BI%~2.json
