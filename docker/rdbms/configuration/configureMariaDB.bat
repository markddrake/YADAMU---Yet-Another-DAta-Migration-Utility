@set CONTAINER_NAME=%1
docker cp docker/rdbms/mariadb/setup                %CONTAINER_NAME%:/var/lib/mysql
docker cp docker/rdbms/mariadb/setup/utf-8.cnf      %CONTAINER_NAME%:/etc/mysql/conf.d
docker cp docker/rdbms/mariadb/testdata             %CONTAINER_NAME%:/var/lib/mysql
docker exec -it -u root -w /                        %CONTAINER_NAME% mkdir -p /var/lib/mysql/sql
docker cp qa/sql/mariadb/YADAMU_TEST.sql            %CONTAINER_NAME%:/var/lib/mysql/sql
docker exec -it -u root -w /                        %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/setup
docker exec -it -u root -w /                        %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/testdata
docker restart                                      %CONTAINER_NAME%
docker exec -it -u root -w /                        %CONTAINER_NAME% mkdir -p /var/lib/mysql/sql
docker exec -it -u root -w /                        %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/sql
docker exec -it                                     %CONTAINER_NAME% /bin/bash /var/lib/mysql/setup/configure.sh
