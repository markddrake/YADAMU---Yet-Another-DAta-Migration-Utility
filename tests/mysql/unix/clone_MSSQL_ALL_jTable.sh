. env/setEnvironment.sh
export DIR=JSON/$MSSQL
export MDIR=$TESTDATA/$MSSQL
export SCHVER=1
mkdir -p $DIR
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <../sql/JSON_IMPORT.sql >$LOGDIR/install/JSON_IMPORT.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=$SCHVER; set @METHOD='JSON_TABLE'" <sql/RECREATE_MSSQL_ALL.sql >>$LOGDIR/RECREATE_SCHEMA.log
. windows/import_MSSQL_jTable.sh $MDIR $SCHVER ""
. windows/export_MSSQL.sh $DIR $SCHVER $SCHVER
export SCHVER=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=$SCHVER; set @METHOD='JSON_TABLE'" <sql/RECREATE_MSSQL_ALL.sql >>$LOGDIR/RECREATE_SCHEMA.log
. windows/import_MSSQL_jTable.sh $DIR $SCHVER 1
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT --init-command="set @ID1=1; set @ID2=2; set @METHOD='JSON_TABLE'" --table  <sql/COMPARE_MSSQL_ALL.sql >>$LOGDIR/COMPARE_SCHEMA.log
. windows/export_MSSQL.sh $DIR $SCHVER $SCHVER
node ../../utilities/node/compareFileSizes $LOGDIR $MDIR $DIR
node ../../utilities/node/compareArrayContent $LOGDIR $MDIR $DIR false