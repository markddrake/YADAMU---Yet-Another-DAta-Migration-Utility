@set CONTAINER_NAME=%1
docker cp docker/rdbms/mysql/setup     %CONTAINER_NAME%:/var/lib/mysql
docker cp docker/rdbms/mysql/testdata  %CONTAINER_NAME%:/var/lib/mysql
docker cp src/install/mysql/sql        %CONTAINER_NAME%:/var/lib/mysql
docker cp qa/sql/mysql/YADAMU_TEST.sql %CONTAINER_NAME%:/var/lib/mysql/sql
docker exec -it -u root -w / %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/setup
docker exec -it -u root -w / %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/testdata
docker exec -it -u root -w / %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/sql
docker exec -it %CONTAINER_NAME% /bin/bash /var/lib/mysql/setup/configure.sh
