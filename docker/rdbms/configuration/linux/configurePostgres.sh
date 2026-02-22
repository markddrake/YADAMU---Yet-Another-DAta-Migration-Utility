export CONTAINER_NAME=$1
docker exec                                              $CONTAINER_NAME mkdir -p /var/lib/postgresql/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/postgres/setup                    $CONTAINER_NAME:/var/lib/postgresql/stage
docker cp src/sql/postgres                               $CONTAINER_NAME:/var/lib/postgresql/stage
docker exec                                              $CONTAINER_NAME mv /var/lib/postgresql/stage/postgres /var/lib/postgresql/stage/sql
docker cp docker/rdbms/postgres/testdata                 $CONTAINER_NAME:/var/lib/postgresql/stage
docker exec                                              $CONTAINER_NAME bash /var/lib/postgresql/stage/setup/configure.sh
docker exec    -u 0                                      $CONTAINER_NAME cp -r /var/lib/postgresql/stage/log /mnt/shared/setup/$CONTAINER_NAME 