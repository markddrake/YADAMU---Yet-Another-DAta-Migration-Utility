. env/setEnvironment.sh
export DIR=JSON/$MSSQL
export MDIR=$TESTDATA/$MSSQL 
export SCHVER=1
mkdir -p $DIR
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @../sql/COMPILE_ALL.sql $LOGDIR 
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/RECREATE_MSSQL_ALL.sql $LOGDIR  $SCHVER Clarinet
sh unix/import_MSSQL.sh $MDIR $SCHVER ""
sh unix/export_MSSQL.sh $DIR $SCHVER $SCHVER
export SCHVER=2
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/RECREATE_MSSQL_ALL.sql $LOGDIR  $SCHVER Clarinet
sh unix/import_MSSQL.sh $DIR $SCHVER 1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/COMPARE_MSSQL_ALL.sql $LOGDIR  1 2 Clarinet $MODE
sh unix/export_MSSQL $DIR $SCHVER $SCHVER
node ../../utilities/compareFileSizes $LOGDIR $MDIR $DIR
node ../../utilities/compareArrayContent $LOGDIR $MDIR $DIR false