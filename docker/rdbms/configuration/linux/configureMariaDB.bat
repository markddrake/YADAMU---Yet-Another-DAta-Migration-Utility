@set CONTAINER_NAME=%1
docker exec -it                                     %CONTAINER_NAME% mkdir -p /var/lib/mysql/stage/sql
docker cp docker/rdbms/mariadb/setup/utf-8.cnf      %CONTAINER_NAME%:/etc/mysql/conf.d
docker cp docker/rdbms/mariadb/setup                %CONTAINER_NAME%:/var/lib/mysql/stage
docker cp docker/rdbms/mariadb/testdata             %CONTAINER_NAME%:/var/lib/mysql/stage
docker cp src/install/mariadb/js                    %CONTAINER_NAME%:/var/lib/mysql/stage
docker cp qa/sql/Mariadb/YADAMU_TEST.sql            %CONTAINER_NAME%:/var/lib/mysql/stage/sql
docker exec -it -u root -w /                        %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/stage
docker exec -it                                     %CONTAINER_NAME% bash /var/lib/mysql/stage/setup/configure.sh
