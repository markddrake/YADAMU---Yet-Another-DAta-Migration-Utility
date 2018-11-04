export TNS=$1
export DIR=JSON/$TNS
export MODE=DATA_ONLY
export MDIR=../../JSON/$TNS/$MODE$
export ID=1
mkdir -p $DIR
. ./env/connection.sh
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f <../sql/JSON_IMPORT.sql
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @ID=1" <sql/RECREATE_oracle_ALL.sql
. ./unix/import_oracle.sh $MDIR $ID ""
. ./unix/export_oracle.sh $DIR $ID $ID
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBASE -P$DB_PORT -v -f --init-command="SET @ID=2" <sql/RECREATE_oracle_ALL.sql
export ID=2
. ./unix/import_oracle.sh $DIR $ID 1
. ./unix/export_oracle.sh $DIR $ID $ID
dir $DIR/*1.json
dir $DIR/*2.json