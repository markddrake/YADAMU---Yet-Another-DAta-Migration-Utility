export CONTAINER_NAME=$1
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker exec  -it                                         $CONTAINER_NAME ./cockroach --host=$CONTAINER_NAME:26357 init --insecure
docker exec  -it                                         $CONTAINER_NAME ./cockroach sql --host=$CONTAINER_NAME --insecure --execute="create database yadamu"
