export SRC=$~1
export USCHEMA=$~2
export SCHVER=$~3
export VER=$~4
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Northwind$VER.json        toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Sales$VER.json            toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Person$VER.json           toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Production$VER.json       toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/Purchasing$VER.json       toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/HumanResources$VER.json   toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
node ../node/import   --username=$DB_USER --hostname=$DB_HOST --password=$DB_PWD  --port=$DB_PORT --database=$DB_DBNAME file=$SRC/AdventureWorksDW$VER.json toUser="$USCHEMA$SCHVER" logFile=$IMPORTLOG
