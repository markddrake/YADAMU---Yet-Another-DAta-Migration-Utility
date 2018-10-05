mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f <SQL/JSON_IMPORT.sql
mysql -u root -poracle -Dsys -h 192.168.1.250 -v -f --init-command="SET @ID=1" <TESTS\RECREATE_SCHEMAS.sql
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"HR1\" --FILE=..\\JSON\\oracle\\18c\\HR_DATA_ONLY.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"SH1\" --FILE=..\\JSON\\oracle\\18c\\SH_DATA_ONLY.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"OE1\" --FILE=..\\JSON\\oracle\\18c\\OE_DATA_ONLY.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"PM1\" --FILE=..\\JSON\\oracle\\18c\\PM_DATA_ONLY.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"IX1\" --FILE=..\\JSON\\oracle\\18c\\IX_DATA_ONLY.json
node node\import --USERNAME=root --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=sys --TOUSER=\"BI1\" --FILE=..\\JSON\\oracle\\18c\\BI_DATA_ONLY.json