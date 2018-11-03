export DIR=JSON/MSSQL
export MDIR=../../JSON/MSSQL 
export ID=1
export SCHEMA=ADVWRK
export FILENAME=AdventureWorks
mkdir $DIR
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f <../../sql/JSON_IMPORT.sql
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=$ID" <sql/RECREATE_MSSQL_ALL.sql
./unix/import_MSSQL_jSax.sh $MDIR$ $ID ""
./unix/export_MSSQL $DIR $ID $ID
export ID=2
mysql -uroot -poracle -h192.168.1.250 -Dsys -P3306 -v -f --init-command="SET @ID=$ID" <sql/RECREATE_MSSQL_ALL.sql
./unix/import_MSSQL_jSax.sh $DIR $ID 1
./unix/export_MSSQL $DIR $ID $ID
dir $DIR/*1.json
dir $DIR/*2.json