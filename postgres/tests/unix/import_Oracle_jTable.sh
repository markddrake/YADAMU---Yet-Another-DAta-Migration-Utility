export SRC=$~1
export SCHVER=$~2
export VER=$~3
node ../node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC$/HR$VER.json toUser="HR$SCHVER$" logFile=$IMPORTLOG mode=$MODE
node ../node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC$/SH$VER.json toUser="SH$SCHVER$" logFile=$IMPORTLOG mode=$MODE
node ../node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC$/OE$VER.json toUser="OE$SCHVER$" logFile=$IMPORTLOG mode=$MODE
node ../node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC$/PM$VER.json toUser="PM$SCHVER$" logFile=$IMPORTLOG mode=$MODE
node ../node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC$/IX$VER.json toUser="IX$SCHVER$" logFile=$IMPORTLOG mode=$MODE
node ../node/jTableImport --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD file=$SRC$/BI$VER.json toUser="BI$SCHVER$" logFile=$IMPORTLOG mode=$MODE

