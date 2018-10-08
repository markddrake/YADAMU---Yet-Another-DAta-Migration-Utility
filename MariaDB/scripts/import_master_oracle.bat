node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"HR%~1\" FILE=..\\JSON\\Oracle\\%~2\\HR_%~3.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"SH%~1\" FILE=..\\JSON\\Oracle\\%~2\\SH_%~3.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"OE%~1\" FILE=..\\JSON\\Oracle\\%~2\\OE_%~3.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"PM%~1\" FILE=..\\JSON\\Oracle\\%~2\\PM_%~3.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"IX%~1\" FILE=..\\JSON\\Oracle\\%~2\\IX_%~3.json
node node\import --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=sys --TOUSER=\"BI%~1\" FILE=..\\JSON\\Oracle\\%~2\\BI_%~3.json
