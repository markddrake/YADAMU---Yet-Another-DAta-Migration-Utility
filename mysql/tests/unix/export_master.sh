export DIR=../../JSON/MYSQL
export SCHEMA=sakila
export FILENAME=sakila
export ID=1node ../node/export --USERNAME=root --HOSTNAME=192.168.1.250 --PORT=3306 --PASSWORD=oracle --DATABASE=sys --File=$DIR/$FILENAME.json owner=$SCHEMA$
