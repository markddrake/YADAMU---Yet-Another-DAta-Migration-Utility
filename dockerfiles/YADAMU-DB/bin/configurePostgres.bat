@set CONTAINER_NAME=%1
docker cp postgres/setup PGSQL12-01:/var/lib/postgresql/data
docker cp ../../app/install/postgres/sql %CONTAINER_NAME%:/var/lib/postgresql/data
docker cp ../../qa/sql/postgres/YADAMU_TEST.sql %CONTAINER_NAME%:/var/lib/postgresql/data/sql
docker exec -it PGSQL12-01 /bin/bash /var/lib/postgresql/data/setup/configure.sh
