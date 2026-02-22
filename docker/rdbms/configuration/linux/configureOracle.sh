export CONTAINER_NAME=$1
docker exec                                              $CONTAINER_NAME mkdir -p /opt/oracle/oradata/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/oracle/setup                      $CONTAINER_NAME:/opt/oracle/oradata/stage
docker cp src/sql/oracle/                                $CONTAINER_NAME:/opt/oracle/oradata/stage
docker exec    -u root                                   $CONTAINER_NAME mv /opt/oracle/oradata/stage/oracle /opt/oracle/oradata/stage/sql
docker cp docker/rdbms/oracle/testdata                   $CONTAINER_NAME:/opt/oracle/oradata/stage
docker exec    -u root -w /opt/oracle/oradata            $CONTAINER_NAME bash -c "command -v microdnf && microdnf install -y dos2unix findutils"
docker exec    -u root -w /opt/oracle/oradata            $CONTAINER_NAME chown -R oracle:oinstall stage
docker exec    -u root -w /opt/oracle/oradata            $CONTAINER_NAME chmod -R u+rwx stage
docker exec                                              $CONTAINER_NAME /bin/bash /opt/oracle/oradata/stage/setup/configure.sh
docker exec    -u 0                                      $CONTAINER_NAME cp -r /opt/oracle/oradata/stage/log /mnt/shared/setup/$CONTAINER_NAME 