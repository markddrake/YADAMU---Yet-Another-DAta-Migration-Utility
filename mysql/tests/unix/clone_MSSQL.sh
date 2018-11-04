export DIR=JSON/MSSQL
export MDIR=../../JSON/MSSQL 
export ID=1
export SCHEMA=ADVWRK
export FILENAME=AdventureWorks
mkdir -p $DIR
. ./env/connection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f <../sql/JSON_IMPORT.sql
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @ID=$ID" <sql/RECREATE_MSSQL_ALL.sql
. ./unix/import_MSSQL.sh $MDIR $ID ""
. ./unix/export_MSSQL $DIR $ID $ID
export ID=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @ID=$ID" <sql/RECREATE_MSSQL_ALL.sql
. ./unix/import_MSSQL.sh $DIR $ID 1
. ./unix/export_MSSQL $DIR $ID $ID
dir $DIR/*1.json
dir $DIR/*2.json