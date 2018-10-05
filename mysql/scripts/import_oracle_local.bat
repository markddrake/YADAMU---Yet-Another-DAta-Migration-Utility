mysql -u root -poracle -Dsys -h 192.168.1.250 -v --init-command="SET @ID=2" <TESTS\RECREATE_SCHEMAS.sql
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"HR2\" --FILE=JSON\\HR1.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"SH2\" --FILE=JSON\\SH1.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"OE2\" --FILE=JSON\\OE1.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"PM2\" --FILE=JSON\\PM1.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"IX2\" --FILE=JSON\\IX1.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"BI2\" --FILE=JSON\\BI1.json