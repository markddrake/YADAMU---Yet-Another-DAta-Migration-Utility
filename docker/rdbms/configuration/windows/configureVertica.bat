@set CONTAINER_NAME=%1
docker exec -it                           %CONTAINER_NAME% mkdir -p /data/vertica/stage
docker cp docker/rdbms/vertica/setup      %CONTAINER_NAME%:/data/vertica/stage
docker cp src/sql/vertica                 %CONTAINER_NAME%:/data/vertica/stage
docker exec -it                           %CONTAINER_NAME% mv /data/vertica/stage/vertica /data/vertica/stage/sql
docker exec -it                           %CONTAINER_NAME% bash /data/vertica/stage/setup/configure.sh
