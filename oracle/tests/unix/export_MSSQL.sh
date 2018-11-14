@SET TGT=$1
@SET SCHVER=$3
@SET VER=$2
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Northwind$SCHVER\"       file=$TGT/Northwind$VER.json        mode=$MODE logfile=$EXPORTLOG
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Sales$SCHVER\"           file=$TGT/Sales$VER.json            mode=$MODE logfile=$EXPORTLOG
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Person$SCHVER\"          file=$TGT/Person$VER.json           mode=$MODE logfile=$EXPORTLOG
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Production$SCHVER\"      file=$TGT/Production$VER.json       mode=$MODE logfile=$EXPORTLOG
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"Purchasing$SCHVER\"      file=$TGT/Purchasing$VER.json       mode=$MODE logfile=$EXPORTLOG
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"HumanResources$SCHVER\"  file=$TGT/HumanResources$VER.json   mode=$MODE logfile=$EXPORTLOG
node ../node/export  userid=$DB_USER/$DB_PWD@$DB_CONNECTION owner=\"DW$SCHVER\"              file=$TGT/AdventureWorksDW$VER.json mode=$MODE logfile=$EXPORTLOG
