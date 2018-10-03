mysql -u root -poracle -Dmdd -h 192.168.1.250 -v --init-command="SET @ID=2" <TESTS\RECREATE_SCHEMAS.sql
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"HR2\" --FILE=JSON\\HR.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"SH2\" --FILE=JSON\\SH.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"OE2\" --FILE=JSON\\OE.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"PM2\" --FILE=JSON\\PM.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"IX2\" --FILE=JSON\\IX.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"BI2\" --FILE=JSON\\BI.json