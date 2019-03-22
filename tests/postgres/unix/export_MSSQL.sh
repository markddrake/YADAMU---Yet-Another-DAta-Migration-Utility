export TGT=$~1
export VER=$~2
export SCHVER=$~3
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="Northwind$SCHVER$"      file=$TGT$/Northwind$VER.json        mode=$MODE logFile=$EXPORTLOG
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="Sales$SCHVER$"          file=$TGT$/Sales$VER.json            mode=$MODE logFile=$EXPORTLOG
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="Person$SCHVER$"         file=$TGT$/Person$VER.json           mode=$MODE logFile=$EXPORTLOG
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="Production$SCHVER$"     file=$TGT$/Production$VER.json       mode=$MODE logFile=$EXPORTLOG
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="Purchasing$SCHVER$"     file=$TGT$/Purchasing$VER.json       mode=$MODE logFile=$EXPORTLOG
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="HumanResources$SCHVER$" file=$TGT$/HumanResources$VER.json   mode=$MODE logFile=$EXPORTLOG
node ../node/export --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD owner="DW$SCHVER$"             file=$TGT$/AdventureWorksDW$VER.json mode=$MODE logFile=$EXPORTLOG
