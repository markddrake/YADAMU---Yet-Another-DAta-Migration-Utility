export SRC=$1
export SCHEMAVER=$2
export VER=$3
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/HR$VER.json toUser=\"HR$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/SH$VER.json toUser=\"SH$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/OE$VER.json toUser=\"OE$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/PM$VER.json toUser=\"PM$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/IX$VER.json toUser=\"IX$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE
node $YADAMU_DB_ROOT/node/import --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC/BI$VER.json toUser=\"BI$SCHEMAVER\" logFile=$IMPORTLOG mode=$MODE

