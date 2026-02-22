export CONTAINER_NAME=$1
docker exec                                              $CONTAINER_NAME mkdir -p /data/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/mongodb/setup                     $CONTAINER_NAME:/data/stage
docker cp src/install/mongodb/js                         $CONTAINER_NAME:/data/stage
docker exec                                              $CONTAINER_NAME bash /data/stage/setup/configure.sh
docker exec    -u 0                                      $CONTAINER_NAME cp -r /data/stage/log /mnt/shared/setup/$CONTAINER_NAME 
