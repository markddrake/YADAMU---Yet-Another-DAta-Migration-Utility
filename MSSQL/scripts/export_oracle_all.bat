node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone OWNER=\"HR%~1\" FILE=JSON\Oracle\HR%~2.json
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone OWNER=\"SH%~1\" FILE=JSON\Oracle\SH%~2.json
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone OWNER=\"OE%~1\" FILE=JSON\Oracle\OE%~2.json
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone OWNER=\"PM%~1\" FILE=JSON\Oracle\PM%~2.json
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone OWNER=\"IX%~1\" FILE=JSON\Oracle\IX%~2.json
node node\export --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --DATABASE=clone OWNER=\"BI%~1\" FILE=JSON\Oracle\BI%~2.json
