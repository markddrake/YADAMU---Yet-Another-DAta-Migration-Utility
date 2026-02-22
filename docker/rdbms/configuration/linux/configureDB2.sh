export CONTAINER_NAME=$1
docker exec                                             $CONTAINER_NAME mkdir -p /database/stage
docker exec                                             YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/db2/setup                        $CONTAINER_NAME:/database/stage
docker cp src/sql/db2/                                  $CONTAINER_NAME:/database/stage
docker exec                                             $CONTAINER_NAME mv /database/stage/db2 /database/stage/sql
docker exec                                             $CONTAINER_NAME chown -R db2inst1:db2iadm1 /database/stage
docker exec -it -u db2inst1                             $CONTAINER_NAME bash /database/stage/setup/configure.sh
docker exec     -u 0                                    $CONTAINER_NAME cp -r /database/stage/log /mnt/shared/setup/$CONTAINER_NAME 