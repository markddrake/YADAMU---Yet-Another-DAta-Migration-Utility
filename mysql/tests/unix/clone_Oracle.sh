export TNS=$1
export DIR=JSON/$TNS
export MODE=DATA_ONLY
export MDIR=../../JSON/$TNS/$MODE
export ID=1
mkdir -p $DIR
. ./env/connection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f <../sql/JSON_IMPORT.sql
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @ID=1" <sql/RECREATE_ORACLE_ALL.sql
. ./unix/import_Oracle.sh $MDIR $ID ""
. ./unix/export_Oracle.sh $DIR $ID $ID
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @ID=2" <sql/RECREATE_ORACLE_ALL.sql
export ID=2
. ./unix/import_Oracle.sh $DIR $ID 1
. ./unix/export_Oracle.sh $DIR $ID $ID
ls -l $DIR/*1.json
ls -l $DIR/*2.json