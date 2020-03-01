@set CONTAINER_NAME=%1
docker cp mysql/setup %CONTAINER_NAME%:/var/lib/mysql
docker exec -it -u root -w / %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/setup
docker cp mysql/testdata %CONTAINER_NAME%:/var/lib/mysql
docker exec -it -u root -w / %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/testdata
docker cp ../../app/install/mysql/sql %CONTAINER_NAME%:/var/lib/mysql
docker cp ../../qa/sql/mysql/YADAMU_TEST.sql %CONTAINER_NAME%:/var/lib/mysql/sql
docker exec -it -u root -w / %CONTAINER_NAME% chown -R mysql:mysql /var/lib/mysql/sql
docker exec -it %CONTAINER_NAME% /bin/bash /var/lib/mysql/setup/configure.sh
