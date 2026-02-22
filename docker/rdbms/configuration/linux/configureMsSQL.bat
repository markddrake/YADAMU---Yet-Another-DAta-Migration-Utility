@set DOCKER_CLI_HINTS=false
@set CONTAINER_NAME=%1
@set MSSQL_VERSION=
@for /f "tokens=*" %%i in ('docker exec %CONTAINER_NAME% /bin/bash -c "grep -m 1 'Microsoft SQL Server' /var/opt/mssql/log/errorlog | awk '{print $7}'"') do set MSSQL_VERSION=%%i
@echo Detected SQL Server %MSSQL_VERSION%
docker exec -it                                          %CONTAINER_NAME% mkdir -p /var/opt/mssql/stage
docker cp docker/rdbms/mssql/setup                       %CONTAINER_NAME%:/var/opt/mssql/stage
docker cp src/sql/mssql/                                 %CONTAINER_NAME%:/var/opt/mssql/stage
docker exec -it                                          %CONTAINER_NAME% mv /var/opt/mssql/stage/mssql /var/opt/mssql/stage/sql 
docker cp docker/rdbms/mssql/testdata/%MSSQL_VERSION%    %CONTAINER_NAME%:/var/opt/mssql/stage/testdata
docker cp docker/rdbms/mssql/testdata/Northwind          %CONTAINER_NAME%:/var/opt/mssql/stage/testdata
docker cp docker/rdbms/mssql/testdata/WideWorldImporters %CONTAINER_NAME%:/var/opt/mssql/stage/testdata
if "%MSSQL_VERSION%" GTR "2017" docker exec -it -u root -w / %CONTAINER_NAME% chown -R mssql:root /var/opt/mssql/stage
docker exec -it                                          %CONTAINER_NAME% bash /var/opt/mssql/stage/setup/configure.sh %MSSQL_VERSION%