export CONTAINER_NAME=$1
docker exec                                              $CONTAINER_NAME mkdir -p /home/yugabyte/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/yugabyte/setup                    $CONTAINER_NAME:/home/yugabyte/stage
docker cp src/sql/postgres/                              $CONTAINER_NAME:/home/yugabyte/stage
docker exec                                              $CONTAINER_NAME mv /home/yugabyte/stage/postgres /home/yugabyte/stage/sql
docker exec                                              $CONTAINER_NAME bash /home/yugabyte/stage/setup/configure.sh
docker exec    -u 0                                      $CONTAINER_NAME cp -r /home/yugabyte/stage/log /mnt/shared/setup/$CONTAINER_NAME 