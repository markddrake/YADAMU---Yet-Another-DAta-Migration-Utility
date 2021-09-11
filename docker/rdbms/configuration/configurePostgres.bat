@set CONTAINER_NAME=%1
docker cp docker/rdbms/postgres/setup    %CONTAINER_NAME%:/var/lib/postgresql/data
docker cp docker/rdbms/postgres/testdata %CONTAINER_NAME%:/var/lib/postgresql/data
docker cp src/install/postgres/sql       %CONTAINER_NAME%:/var/lib/postgresql/data
docker cp qa/sql/postgres/YADAMU_TEST.sql %CONTAINER_NAME%:/var/lib/postgresql/data/sql
docker exec -it %CONTAINER_NAME% /bin/bash /var/lib/postgresql/data/setup/configure.sh
