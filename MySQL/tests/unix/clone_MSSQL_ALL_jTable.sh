export DIR=JSON/MSSQL
export MDIR=../../JSON/MSSQL 
export ID=1
export SCHEMA=ADVWRK
export FILENAME=AdventureWorks
mkdir $DIR
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <../sql/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @SCHEMA='$SCHEMA'; SET @ID=$ID" <sql/RECREATE_SCHEMA.sql
./unix/import_MSSQL_ALL_jTable.sh $MDIR$ $SCHEMA$ $ID "" 
node ../node/export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=$DIR/$FILENAME$ID.json owner=$SCHEMA$ID
export ID=2
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @SCHEMA='$SCHEMA'; SET @ID=2" <sql/RECREATE_SCHEMA.sql
node ../node/jTableImport --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=$DIR/$FILENAME$1.json toUser=$SCHEMA$ID
node ../node/export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=$DIR/$FILENAME$ID.json owner=$SCHEMA$ID
dir $DIR/*1.json
dir $DIR/*2.json