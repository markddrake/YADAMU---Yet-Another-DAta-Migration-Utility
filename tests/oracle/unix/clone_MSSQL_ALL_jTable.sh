. env/setEnvironment.sh
export DIR=JSON/$MSSQL
export MDIR=$TESTDATA/$MSSQL 
export SCHVER=1
mkdir -p $DIR
. env/connection.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @../sql/COMPILE_ALL.sql $LOGDIR 
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/RECREATE_MSSQL_ALL.sql $LOGDIR  $SCHVER JSON_TABLE
sh unix/import_MSSQL_jTable.sh $MDIR $SCHVER ""
sh unix/export_MSSQL.sh $DIR $SCHVER $SCHVER
export SCHVER=2
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/RECREATE_MSSQL_ALL.sql $LOGDIR  $SCHVER JSON_TABLE
sh unix/import_MSSQL_jTable.sh $DIR $SCHVER 1
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @sql/COMPARE_MSSQL_ALL.sql $LOGDIR  1 2 JSON_TABLE $MODE
sh unix/export_MSSQL $DIR $SCHVER $SCHVER
node ../../utilities/node/compareFileSizes $LOGDIR $MDIR $DIR
node ../../utilities/node/compareArrayContent $LOGDIR $MDIR $DIR false