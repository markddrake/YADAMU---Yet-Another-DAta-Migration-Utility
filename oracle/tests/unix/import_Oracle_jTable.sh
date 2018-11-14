export SRC=$1
export SCHVER=$2
export VER=$3
node ../node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/HR$VER.json toUser=\"HR$SCHVER\" logfile=$IMPORTLOG
node ../node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/SH$VER.json toUser=\"SH$SCHVER\" logfile=$IMPORTLOG
node ../node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/OE$VER.json toUser=\"OE$SCHVER\" logfile=$IMPORTLOG
node ../node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/PM$VER.json toUser=\"PM$SCHVER\" logfile=$IMPORTLOG
node ../node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/IX$VER.json toUser=\"IX$SCHVER\" logfile=$IMPORTLOG
node ../node/jTableImport userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$SRC/BI$VER.json toUser=\"BI$SCHVER\" logfile=$IMPORTLOG
