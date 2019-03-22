export TGT=$1
export SCHVER=$2
export VER=$3
node ../node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/HR$VER.json owner=\"HR$SCHVER\" mode=$MODE logfile=$EXPORTLOG mode=$MODE 
node ../node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/SH$VER.json owner=\"SH$SCHVER\" mode=$MODE logfile=$EXPORTLOG mode=$MODE 
node ../node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/OE$VER.json owner=\"OE$SCHVER\" mode=$MODE logfile=$EXPORTLOG mode=$MODE 
node ../node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/PM$VER.json owner=\"PM$SCHVER\" mode=$MODE logfile=$EXPORTLOG mode=$MODE 
node ../node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/IX$VER.json owner=\"IX$SCHVER\" mode=$MODE logfile=$EXPORTLOG mode=$MODE 
node ../node/export userid=$DB_USER/$DB_PWD@$DB_CONNECTION file=$TGT/BI$VER.json owner=\"BI$SCHVER\" mode=$MODE logfile=$EXPORTLOG mode=$MODE 
