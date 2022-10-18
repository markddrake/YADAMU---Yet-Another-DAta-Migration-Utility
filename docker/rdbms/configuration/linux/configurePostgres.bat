@set CONTAINER_NAME=%1
docker exec -it                           %CONTAINER_NAME% mkdir -p /var/lib/postgresql/stage
docker cp docker/rdbms/postgres/setup     %CONTAINER_NAME%:/var/lib/postgresql/stage
docker cp src/sql/postgres                %CONTAINER_NAME%:/var/lib/postgresql/stage
docker exec -it                           %CONTAINER_NAME% mv /var/lib/postgresql/stage/postgres /var/lib/postgresql/stage/sql
docker cp docker/rdbms/postgres/testdata  %CONTAINER_NAME%:/var/lib/postgresql/stage
docker exec -it                           %CONTAINER_NAME% bash /var/lib/postgresql/stage/setup/configure.sh
