export YADAMU_HOME=`pwd`
cd oracle/node
rm -rf node_modules
npm install oracledb
cd $YADAMU_HOME
cd mssql/node
rm -rf node_modules
npm install mssql
cd $YADAMU_HOME
cd postgres/node
rm -rf node_modules
npm install pg
npm install pg-copy-streams
npm install pg-query-stream
cd $YADAMU_HOME
cd mysql/node
rm -rf node_modules
npm install mysql
npm install wkx
cd $YADAMU_HOME
cd mariadb/node
rm -rf node_modules
npm install mariadb
cd $YADAMU_HOME
cd mongodb/node
rm -rf node_modules
npm install mongodb
cd $YADAMU_HOME
