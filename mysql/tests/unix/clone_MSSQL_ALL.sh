export DIR=JSON/MSSQL
export MDIR=../../JSON/MSSQL 
export ID=1
export SCHEMA=ADVWRK
export FILENAME=AdventureWorks
mkdir -p $DIR
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f <../sql/JSON_IMPORT.sql
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @SCHEMA='$SCHEMA'; SET @ID=$ID" <sql/RECREATE_SCHEMA.sql
. ./unix/import_MSSQL_ALL.sh $MDIR $SCHEMA$ $ID "" 
node ../node/export --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PORT=$DB_PORT --PASSWORD=$DB_PWD --DATABASE=$DB_DBASE --File=$DIR/$FILENAME$ID.json owner=$SCHEMA$ID
export ID=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @SCHEMA='$SCHEMA'; SET @ID=2" <sql/RECREATE_SCHEMA.sql
node ../node/import --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PORT=$DB_PORT --PASSWORD=$DB_PWD --DATABASE=$DB_DBASE --File=$DIR/$FILENAME$1.json toUser=$SCHEMA$ID
node ../node/export --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PORT=$DB_PORT --PASSWORD=$DB_PWD --DATABASE=$DB_DBASE --File=$DIR/$FILENAME$ID.json owner=$SCHEMA$ID
dir $DIR/*1.json
dir $DIR/*2.json