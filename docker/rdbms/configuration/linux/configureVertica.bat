@set CONTAINER_NAME=%1
docker exec -it                           %CONTAINER_NAME% mkdir -p /data/vertica/stage
docker cp docker/rdbms/vertica/setup      %CONTAINER_NAME%:/data/vertica/stage
docker cp src/install/vertica/sql         %CONTAINER_NAME%:/data/vertica/stage
docker cp qa/sql/vertica/YADAMU_TEST.sql  %CONTAINER_NAME%:/data/vertica/stage/sql
docker exec -it                           %CONTAINER_NAME% bash /data/vertica/stage/setup/configure.sh
