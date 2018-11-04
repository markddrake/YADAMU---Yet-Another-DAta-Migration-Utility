export DIR=JSON/MSSQL
export MDIR=../../JSON/MSSQL 
export ID=1
export SCHEMA=ADVWRK
export FILENAME=AdventureWorks
mkdir -p $DIR
. ./env/connection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <../sql/JSON_IMPORT.sql
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="SET @ID=$ID" <sql/RECREATE_MSSQL_ALL.sql
. ./unix/import_MSSQL_jTable.sh $MDIR $ID ""
. ./unix/export_MSSQL.sh $DIR $ID $ID
export ID=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="SET @ID=$ID" <sql/RECREATE_MSSQL_ALL.sql
. ./unix/import_MSSQL_jTable.sh $DIR $ID 1
. ./unix/export_MSSQL.sh $DIR $ID $ID
ls -l $DIR/*1.json
ls -l $DIR/*2.json