node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  OWNER=\"HR%~1\" FILE=JSON\Oracle\18c\HR%~2.json
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  OWNER=\"SH%~1\" FILE=JSON\Oracle\18c\SH%~2.json
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  OWNER=\"OE%~1\" FILE=JSON\Oracle\18c\OE%~2.json
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  OWNER=\"PM%~1\" FILE=JSON\Oracle\18c\PM%~2.json
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  OWNER=\"IX%~1\" FILE=JSON\Oracle\18c\IX%~2.json
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle  OWNER=\"BI%~1\" FILE=JSON\Oracle\18c\BI%~2.json
