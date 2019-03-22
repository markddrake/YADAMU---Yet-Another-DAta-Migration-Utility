@SET TGT=$1
@SET SCHVER=$3
@SET VER=$2
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Northwind$SCHVER\"       file=$TGT/Northwind$VER.json        logfile=$EXPORTLOG mode=$MODE 
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Sales$SCHVER\"           file=$TGT/Sales$VER.json            logfile=$EXPORTLOG mode=$MODE 
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Person$SCHVER\"          file=$TGT/Person$VER.json           logfile=$EXPORTLOG mode=$MODE 
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Production$SCHVER\"      file=$TGT/Production$VER.json       logfile=$EXPORTLOG mode=$MODE 
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Purchasing$SCHVER\"      file=$TGT/Purchasing$VER.json       logfile=$EXPORTLOG mode=$MODE 
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"HumanResources$SCHVER\"  file=$TGT/HumanResources$VER.json   logfile=$EXPORTLOG mode=$MODE 
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"DW$SCHVER\"              file=$TGT/AdventureWorksDW$VER.json logfile=$EXPORTLOG mode=$MODE 
