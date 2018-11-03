export TNS=$~1
export DIR=JSON/$TNS$
export MODE=DATA_ONLY
export MDIR=../../JSON/$TNS/$MODE$
export ID=1
mkdir $DIR
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <../sql/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=1" <sql/RECREATE_ORACLE_ALL.sql
call windows/import_oracle_jSax.sh $MDIR$ $ID ""
call windows/export_oracle.sh $DIR $ID $ID
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=2" <sql/RECREATE_ORACLE_ALL.sql
export ID=2
call windows/import_oracle_jSax.sh $DIR $ID 1
call windows/export_oracle.sh $DIR $ID $ID
dir $DIR/*1.json
dir $DIR/*2.json