@set CONTAINER_NAME=%1
docker exec -it                           %CONTAINER_NAME% mkdir -p /database/stage
docker cp docker/rdbms/db2/setup          %CONTAINER_NAME%:/database/stage
docker cp src/sql/db2/                    %CONTAINER_NAME%:/database/stage
docker exec -it                           %CONTAINER_NAME% mv /database/stage/db2 /database/stage/sql
docker exec -it -u db2inst1               %CONTAINER_NAME% bash /database/stage/setup/configure.sh
