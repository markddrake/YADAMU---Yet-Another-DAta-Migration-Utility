. env/setEnvironment.sh
export DIR=JSON/$ORCL
export MDIR=$TESTDATA/$ORCL/$MODE
export SCHVER=1
mkdir -p $DIR
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f <../sql/SCHEMA_COMPARE.sql >$LOGDIR/install/SCHEMA_COMPARE.log
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=$SCHVER; set @METHOD='Clarinet';"<sql/RECREATE_ORACLE_ALL.sql >>$LOGDIR/RECREATE_SCHEMA.log
. windows/import_Oracle.sh $MDIR $SCHVER ""
. windows/export_Oracle.sh $DIR $SCHVER $SCHVER $MODE
export SCHVER=2
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT -v -f --init-command="set @ID=$SCHVER; set @METHOD='Clarinet';"<sql/RECREATE_ORACLE_ALL.sql>>$LOGDIR/RECREATE_SCHEMA.log
. windows/import_Oracle.sh $DIR $SCHVER 1 
mysql -u$DB_USER -p$DB_PWD -h$DB_HOST -D$DB_DBNAME -P$DB_PORT --init-command="set @ID1=1; set @ID2=$SCHVER; set @METHOD='Clarinet'" --table  <sql/COMPARE_ORACLE_ALL.sql >>$LOGDIR/COMPARE_SCHEMA.log
. windows/export_Oracle.sh $DIR $SCHVER $SCHVER $MODE 
node ../../utilities/compareFileSizes $LOGDIR $MDIR $DIR
node ../../utilities/compareArrayContent $LOGDIR $MDIR $DIR false