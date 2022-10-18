@set CONTAINER_NAME=%1
docker exec -it                        %CONTAINER_NAME% mkdir -p /var/lib/mysql/stage
docker cp docker/rdbms/mysql/setup     %CONTAINER_NAME%:/var/lib/mysql/stage
docker cp src/sql/mysql/               %CONTAINER_NAME%:/var/lib/mysql/stage
docker exec -it                        %CONTAINER_NAME% mv /var/lib/mysql/stage/mysql /var/lib/mysql/stage/sql 
docker cp docker/rdbms/mysql/testdata  %CONTAINER_NAME%:/var/lib/mysql/stage
docker cp src/install/mysql/js         %CONTAINER_NAME%:/var/lib/mysql/stage
docker exec -it -u root -w /           %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/stage
docker exec -it                        %CONTAINER_NAME% bash /var/lib/mysql/stage/setup/configure.sh
