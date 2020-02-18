@set CONTAINER_NAME=%1
docker cp mssql/setup %CONTAINER_NAME%:/var/opt/mssql
if "%CONTAINER_NAME%" GTR "MSSQL19" docker exec -it -u root -w / %CONTAINER_NAME% chown -R mssql:root /var/opt/mssql/setup
docker cp mssql/testdata %CONTAINER_NAME%:/var/opt/mssql
if "%CONTAINER_NAME%" GTR "MSSQL19"  docker exec -it -u root -w / %CONTAINER_NAME% chown -R mssql:root /var/opt/mssql/testdata
docker cp ../../app/install/mssql/sql %CONTAINER_NAME%:/var/opt/mssql
docker cp ../../qa/sql/mssql/YADAMU_TEST.sql %CONTAINER_NAME%:/var/opt/mssql/sql
if "%CONTAINER_NAME%" GTR "MSSQL19"  docker exec -it -u root -w / %CONTAINER_NAME% chown -R mssql:root /var/opt/mssql/sql
docker exec -it %CONTAINER_NAME% /bin/bash /var/opt/mssql/setup/configure.sh