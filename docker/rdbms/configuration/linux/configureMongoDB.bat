@set CONTAINER_NAME=%1
docker exec -it                           %CONTAINER_NAME% mkdir -p /data/stage
docker cp docker/rdbms/mongodb/setup      %CONTAINER_NAME%:/data/stage
docker cp src/install/mongodb/js          %CONTAINER_NAME%:/data/stage
docker exec -it                           %CONTAINER_NAME% bash /data/stage/setup/configure.sh
