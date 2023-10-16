export CONTAINER_NAME=$1
docker exec -it                        $CONTAINER_NAME mkdir -p /var/opt/mssql/stage
docker cp docker/rdbms/mssql/setup     $CONTAINER_NAME:/var/opt/mssql/stage
docker cp src/sql/mssql/               $CONTAINER_NAME:/var/opt/mssql/stage
docker exec -it                        $CONTAINER_NAME mv /var/opt/mssql/stage/mssql /var/opt/mssql/stage/sql 
docker cp docker/rdbms/mssql/testdata  $CONTAINER_NAME:/var/opt/mssql/stage
if [[ "$CONTAINER_NAME" > "MSSQL19" ]]; then docker exec -it -u root -w / $CONTAINER_NAME chown -R mssql:root /var/opt/mssql/stage; fi
docker exec -it                        $CONTAINER_NAME bash /var/opt/mssql/stage/setup/configure.sh