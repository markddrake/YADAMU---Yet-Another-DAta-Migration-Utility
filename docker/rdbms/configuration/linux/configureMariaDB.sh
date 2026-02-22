export CONTAINER_NAME=$1
docker exec                                              $CONTAINER_NAME mkdir -p /var/lib/mysql/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/mariadb/setup/utf-8.cnf           $CONTAINER_NAME:/etc/mysql/conf.d
docker cp src/sql/mariadb/                               $CONTAINER_NAME:/var/lib/mysql/stage
docker exec                                              $CONTAINER_NAME mv /var/lib/mysql/stage/mariadb /var/lib/mysql/stage/sql
docker cp docker/rdbms/mariadb/setup                     $CONTAINER_NAME:/var/lib/mysql/stage
docker cp docker/rdbms/mariadb/testdata                  $CONTAINER_NAME:/var/lib/mysql/stage
docker cp src/install/mariadb/js/YADAMU_INSTALL.js       $CONTAINER_NAME:/var/lib/mysql/stage/setup
docker exec    -u root -w /                              $CONTAINER_NAME chown -R mysql:mysql /var/lib/mysql/stage
docker exec                                              $CONTAINER_NAME bash /var/lib/mysql/stage/setup/configure.sh
docker exec    -u 0                                      $CONTAINER_NAME cp -r /var/lib/mysql/stage/log /mnt/shared/setup/$CONTAINER_NAME 