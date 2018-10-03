mysql -u root -poracle -Dmdd -h 192.168.1.250 -v --init-command="SET @ID=1" <TESTS\RECREATE_SCHEMAS.sql
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"HR1\" --FILE=..\\JSON\\oracle\\18c\\HR.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"SH1\" --FILE=..\\JSON\\oracle\\18c\\SH.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"OE1\" --FILE=..\\JSON\\oracle\\18c\\OE.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"PM1\" --FILE=..\\JSON\\oracle\\18c\\PM.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"IX1\" --FILE=..\\JSON\\oracle\\18c\\IX.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=mdd --TOUSER=\"BI1\" --FILE=..\\JSON\\oracle\\18c\\BI.json