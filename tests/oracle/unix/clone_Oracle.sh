. env/setEnvironment.sh
export DIR=JSON/$ORCL
export MDIR=$TESTDATA/$ORCL/$MODE
mkdir -p $DIR
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @../sql/COMPILE_ALL.sql $LOGDIR 
export SCHVER=1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/RECREATE_ORACLE_ALL.sql $LOGDIR  $SCHVER Clarinet $MODE
sh unix/import_Oracle.sh $MDIR $SCHVER ""
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/COMPARE_ORACLE_ALL.sql $LOGDIR  "" 1 Clarinet $MODE
sh unix/export_Oracle.sh $DIR $SCHVER $SCHVER $MODE
export SCHVER=2
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/RECREATE_ORACLE_ALL.sql $LOGDIR  $SCHVER Clarinet $MODE
sh unix/import_Oracle.sh $DIR $SCHVER 1 
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/COMPARE_ORACLE_ALL.sql $LOGDIR  1 2 Clarinet $MODE
sh unix/export_Oracle.sh $DIR $SCHVER $SCHVER $MODE 
node ../../utilities/node/compareFileSizes $LOGDIR $MDIR $DIR
node ../../utilities/node/compareArrayContent $LOGDIR $MDIR $DIR false