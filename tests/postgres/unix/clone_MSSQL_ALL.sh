. env/setEnvironment.bat
export DIR=JSON/$MSSQL
export MDIR=$TESTDATA$/$MSSQL
export SCHVER=1
mkdir -p $DIR
psql -U $DB_USER -h $DB_HOST -a -f ../sql/JSON_IMPORT.sql >> $LOGDIR$/install/JSON_IMPORT.log
psql -U $DB_USER -h $DB_HOST -a -vID=$SCHVER -vMETHOD=Clarinet -f sql/RECREATE_MSSQL_ALL.sql >>$LOGDIR$/RECREATE_SCHEMA.log
. windows/import_MSSQL.bat $MDIR $SCHVER ""
. windows/export_MSSQL $DIR $SCHVER $SCHVER
export SCHVER=2
psql -U $DB_USER -h $DB_HOST -a -vID=$SCHVER -vMETHOD=Clarinet -f sql/RECREATE_MSSQL_ALL.sql >>$LOGDIR$/RECREATE_SCHEMA.log
. windows/import_MSSQL.bat $DIR $SCHVER 1
psql -U $DB_USER -h $DB_HOST -q -vID1=1 -vID2=2 -vMETHOD=Clarinet -f sql/COMPARE_MSSQL_ALL.sql >>$LOGDIR$/COMPARE_SCHEMA.log
. windows/export_MSSQL $DIR $SCHVER $SCHVER
node ../../utilities/node/compareFileSizes $LOGDIR $MDIR $DIR
node ../../utilities/node/compareArrayContent $LOGDIR $MDIR $DIR false