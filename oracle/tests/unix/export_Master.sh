. env/setEnvironment.sh
sqlplus $DB_USER/$DB_PWD@$DB_CONNECTION @../sql/COMPILE_ALL.sql $LOGDIR 
export MODE=DDL_ONLY
export MDIR=$TESTDATA/$ORCL/$MODE
mkdir -p $MDIR
sh unix/export_Oracle.sh $MDIR "" "" $MODE logfile=$EXPORTLOG
export MODE=DATA_ONLY
export MDIR=$TESTDATA/$ORCL/$MODE
mkdir -p $MDIR
sh unix/export_Oracle.sh  $MDIR "" "" $MODE logfile=$EXPORTLOG
export MODE=DDL_AND_DATA
export MDIR=$TESTDATA/$ORCL/$MODE
mkdir -p $MDIR
sh unix/export_Oracle.sh  $MDIR "" "" $MODE logfile=$EXPORTLOG
