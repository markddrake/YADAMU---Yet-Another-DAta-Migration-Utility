if [ ! -z ${MODE+x} ]; then set MODE=DATA_ONLY;fi
export YADAMU_TARGET=MsSQL
. ../unix/initialize.sh $(readlink -f "$BASH_SOURCE")
if [ ! -e $YADAMU_INPUT_PATH]; then mkdir -p $YADAMU_INPUT_PATH; fi
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i$YADAMU_DB_ROOT/sql/YADAMU_IMPORT.sql > $YADAMU_LOG_PATH/install/YADAMU_IMPORT.log
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -e -i$YADAMU_SCRIPT_ROOT/sql/YADAMU_TEST.sql > $YADAMU_LOG_PATH/install/YADAMU_TEST.log
. $YADAMU_SCRIPT_ROOT/unix/export_MSSQL.sh $YADAMU_INPUT_PATH "" ""
export FILENAME=AdventureWorksALL
export MSSQL_SCHEMA=ADVWRK
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -d$DB_DBNAME -I -e -i$YADAMU_SCRIPT_ROOT/sql/RECREATE_SCHEMA.sql >>$YADAMU_LOG_PATH/RECREATE_SCHEMA.log
. $YADAMU_SCRIPT_ROOT/unix/import_MSSQL_ALL.sh $YADAMU_INPUT_PATH "" "" 
node $YADAMU_DB_ROOT/node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD --database=$SCHEMA file=$YADAMU_INPUT_PATH/$FILENAME.json owner=\"dbo\" mode=$MODE log_file=$EXPORTLOG