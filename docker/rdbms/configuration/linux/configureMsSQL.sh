export DOCKER_CLI_HINTS=false
export CONTAINER_NAME=$1
unset MSSQL_VERSION
MSSQL_VERSION=$(docker exec "$CONTAINER_NAME" bash -c "grep -m 1 'Microsoft SQL Server' /var/opt/mssql/log/errorlog | awk '{print \$7}'")
echo "Detected SQL Server $MSSQL_VERSION"
docker exec                                              $CONTAINER_NAME mkdir -p /var/opt/mssql/stage
docker exec                                              YADAMU-SMB      mkdir -p /mount/setup/$CONTAINER_NAME 
docker cp docker/rdbms/mssql/setup                       $CONTAINER_NAME:/var/opt/mssql/stage
docker cp src/sql/mssql/                                 $CONTAINER_NAME:/var/opt/mssql/stage
docker exec                                              $CONTAINER_NAME mv /var/opt/mssql/stage/mssql /var/opt/mssql/stage/sql 
docker cp docker/rdbms/mssql/testdata/$MSSQL_VERSION     $CONTAINER_NAME:/var/opt/mssql/stage/testdata
docker cp docker/rdbms/mssql/testdata/Northwind          $CONTAINER_NAME:/var/opt/mssql/stage/testdata
docker cp docker/rdbms/mssql/testdata/WideWorldImporters $CONTAINER_NAME:/var/opt/mssql/stage/testdata
if [[ "$MSSQL_VERSION" > "2017" ]]; then docker exec     -u root -w / $CONTAINER_NAME chown -R mssql:root /var/opt/mssql/stage; fi
docker exec                                              $CONTAINER_NAME bash /var/opt/mssql/stage/setup/configure.sh $MSSQL_VERSION
docker exec    -u 0                                      $CONTAINER_NAME cp -r /var/opt/mssql/stage/log /mnt/shared/setup/$CONTAINER_NAME 