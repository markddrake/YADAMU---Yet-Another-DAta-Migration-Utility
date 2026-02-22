export STAGE=/data/stage
cd $STAGE
mkdir -p log
mongosh --host localhost --port 27017 --file /data/stage/js/yadamuInstanceID.js