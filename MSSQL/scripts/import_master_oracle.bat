node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=clone TOUSER=\"HR%~1\" FILE=..\JSON\Oracle\%~2\HR_%~3.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=clone TOUSER=\"SH%~1\" FILE=..\JSON\Oracle\%~2\SH_%~3.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=clone TOUSER=\"OE%~1\" FILE=..\JSON\Oracle\%~2\OE_%~3.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=clone TOUSER=\"PM%~1\" FILE=..\JSON\Oracle\%~2\PM_%~3.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=clone TOUSER=\"IX%~1\" FILE=..\JSON\Oracle\%~2\IX_%~3.json
node node\import --USERNAME=sa --HOSTNAME=192.168.1.250 --PASSWORD=oracle --DATABASE=clone TOUSER=\"BI%~1\" FILE=..\JSON\Oracle\%~2\BI_%~3.json
