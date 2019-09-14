export TGT=$1
export VER=$2
export SCHEMAVER=$3
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"Northwind$SCHEMAVER\"       file=$TGT/Northwind$VER.json        mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"Sales$SCHEMAVER\"           file=$TGT/Sales$VER.json            mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"Person$SCHEMAVER\"          file=$TGT/Person$VER.json           mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"Production$SCHEMAVER\"      file=$TGT/Production$VER.json       mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"Purchasing$SCHEMAVER\"      file=$TGT/Purchasing$VER.json       mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"HumanResources$SCHEMAVER\"  file=$TGT/HumanResources$VER.json   mode=$MODE log_file=$EXPORTLOG
node $YADAMU_DB_ROOT/node/export   --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PASSWORD=$DB_PWD  --PORT=$DB_PORT --DATABASE=$DB_DBNAME owner=\"AdventureWorksDW$SCHEMAVER\"              file=$TGT/AdventureWorksDW$VER.json mode=$MODE log_file=$EXPORTLOG
