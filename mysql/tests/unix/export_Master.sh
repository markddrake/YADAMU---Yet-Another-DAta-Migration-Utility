export DIR=../../JSON/MYSQL
export SCHEMA=sakila
export FILENAME=sakila
. ./env/connection.sh
node ../node/export --USERNAME=$DB_USER --HOSTNAME=$DB_HOST --PORT=$DB_PORT --PASSWORD=$DB_PWD --DATABASE=$DB_DBNAME --File=$DIR/$FILENAME.json owner=$SCHEMA
