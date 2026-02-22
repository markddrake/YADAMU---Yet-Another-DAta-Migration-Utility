export CONTAINER_NAME=$1
docker exec                                              $CONTAINER_NAME mkdir -p /data/vertica/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/vertica/setup                     $CONTAINER_NAME:/data/vertica/stage
docker cp src/sql/vertica                                $CONTAINER_NAME:/data/vertica/stage
docker exec                                              $CONTAINER_NAME mv /data/vertica/stage/vertica /data/vertica/stage/sql
docker exec                                              $CONTAINER_NAME bash /data/vertica/stage/setup/configure.sh
docker exec    -u 0                                      $CONTAINER_NAME cp -r /data/vertica/stage/log /mnt/shared/setup/$CONTAINER_NAME 