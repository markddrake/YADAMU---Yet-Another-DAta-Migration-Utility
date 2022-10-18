@set CONTAINER_NAME=%1
docker exec -it                           %CONTAINER_NAME% mkdir -p /home/yugabyte/stage
docker cp docker/rdbms/yugabyte/setup     %CONTAINER_NAME%:/home/yugabyte/stage
docker cp src/sql/postgres/               %CONTAINER_NAME%:/home/yugabyte/stage
docker exec -it                           %CONTAINER_NAME% mv /home/yugabyte/stage/postgres /home/yugabyte/stage/sql
docker exec -it                           %CONTAINER_NAME% bash /home/yugabyte/stage/setup/configure.sh