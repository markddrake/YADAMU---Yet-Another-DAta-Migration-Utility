@set CONTAINER_NAME=%1
docker exec -it                        %CONTAINER_NAME% mkdir -p /var/opt/mssql/stage
docker cp docker/rdbms/mssql/setup     %CONTAINER_NAME%:/var/opt/mssql/stage
docker cp docker/rdbms/mssql/testdata  %CONTAINER_NAME%:/var/opt/mssql/stage
docker cp src/install/mssql/sql        %CONTAINER_NAME%:/var/opt/mssql/stage
docker cp qa/sql/mssql/YADAMU_TEST.sql %CONTAINER_NAME%:/var/opt/mssql/stage/sql
if "%CONTAINER_NAME%" GTR "MSSQL19" docker exec -it -u root -w / %CONTAINER_NAME% chown -R mssql:root /var/opt/mssql/stage
docker exec -it                        %CONTAINER_NAME% bash /var/opt/mssql/stage/setup/configure.sh