set DCONFIG=%CD%
for /f "usebackq tokens=1 delims=," %%a in ("%DCONFIG%\bin\volumes.csv") do (
  docker volume create %%a
)
docker volume ls
docker network create YADAMU-NET
REM Oracle 19.3.0.0.0
docker run --name ORA1903-01 --memory="16g" --shm-size=4g -p 1521:1521 -e ORACLE_SID=CDB19300 -e ORACLE_PDB=PDB19300 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET=AL32UTF8 -vORA1903-01-DATA:/opt/oracle/oradata -vORA1903-01-DIAG:/opt/oracle/diag -vORA1903-01-DIAG:/opt/oracle/admin --tmpfs /dev/shm/:rw,nosuid,nodev,exec,size=4g -d oracle/database:19.3.0-ee
REM Oracle 18.3.0.0.0                                                                                                                                                        
docker run --name ORA1803-01 --memory="16g" --shm-size=4g -p 1522:1521 -e ORACLE_SID=CDB18300 -e ORACLE_PDB=PDB18300 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET=AL32UTF8 -vORA1803-01-DATA:/opt/oracle/oradata -vORA1803-01-DIAG:/opt/oracle/diag -vORA1803-01-DIAG:/opt/oracle/admin --tmpfs /dev/shm/:rw,nosuid,nodev,exec,size=4g -d oracle/database:18.3.0-ee
REM Oracle 12.2.0.1.0                                                                                                                                                        
docker run --name ORA1220-01 --memory="16g" --shm-size=4g -p 1523:1521 -e ORACLE_SID=CDB12200 -e ORACLE_PDB=PDB12200 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET=AL32UTF8 -vORA1220-01-DATA:/opt/oracle/oradata -vORA1220-01-DIAG:/opt/oracle/diag -vORA1220-01-DIAG:/opt/oracle/admin --tmpfs /dev/shm/:rw,nosuid,nodev,exec,size=4g -d oracle/database:12.2.0.1-ee 
REM Oracle 12.1.0.2.0                                                                                                                                                        
docker run --name ORA1210-01 --memory="16g" --shm-size=4g -p 1524:1521 -e ORACLE_SID=CDB12100 -e ORACLE_PDB=PDB12100 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET=AL32UTF8 -vORA1210-01-DATA:/opt/oracle/oradata -vORA1210-01-DIAG:/opt/oracle/diag -vORA1210-01-DIAG:/opt/oracle/admin --tmpfs /dev/shm/:rw,nosuid,nodev,exec,size=4g -d oracle/database:12.1.0.2-ee 
REM Fix for Oracle 11.2..0.1.0 persmissions
set /P DOCKER_USER=docker 
ssh %DOCKER_USER%@%DOCKER_IP_ADDR% "sudo chown 54321:54322 /var/lib/docker/volumes/ORA1120-01-DIAG/_data"
REM Oracle 11.2.0.1.0                                                                                                                                                        
docker run --name ORA1120-01 --memory="16g" --shm-size=4g -p 1525:1521 -e ORACLE_SID=ORA11200 -e ORACLE_PDB=ORA11200 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET=AL32UTF8 -vORA1120-01-DATA:/opt/oracle/oradata -vORA1120-01-DIAG:/opt/oracle/diag -vORA1120-01-DIAG:/opt/oracle/admin  --tmpfs /dev/shm/:rw,nosuid,nodev,exec,size=4g -d oracle/database:11.2.0.1-ee
REM Oracle MySQL 8.0
docker run --name MYSQL80-01 --memory="16g" --shm-size=4g -p 3306:3306 -v MYSQL80-01-DATA:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=oracle --cap-add=sys_nice -d mysql:latest 
REM MaraDB 10.0
docker run --name MARIA10-01 --memory="16g" --shm-size=4g -p 3307:3306 -v MARIA10-01-DATA:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=oracle -d mariadb:latest  
REM MsSQL 2017
docker run --name MSSQL17-01 --memory="48g" --shm-size=4g -p 1433:1433 -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" -e "MSSQL_MEMORY_LIMIT_MB=16384" -v MSSQL17-01-DATA:/var/opt/mssql -d mcr.microsoft.com/mssql/server:2017-latest
REM MsSQL 2019
docker run --name MSSQL19-01 --memory="48g" --shm-size=4g -p 1434:1433 -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" -e "MSSQL_MEMORY_LIMIT_MB=16384" -v MSSQL19-01-DATA:/var/opt/mssql -d mcr.microsoft.com/mssql/server:2019-latest
REM Postgres 12
docker run --name PGSQL12-01 --memory="16g" --shm-size=4g -p 5432:5432 -e POSTGRES_PASSWORD=oracle -v PGSQL12-01-DATA:/var/lib/postgresql/data -d postgres:latest 
REM MongoDB 4.0
docker run --name MONGO40-01 --memory="16g" --shm-size=4g -p 27017:27017 -v MONGO40-01-DATA:/data/db -d mongo:latest 
docker system df
for /f "usebackq tokens=1 delims=," %%a in ("%DCONFIG%\bin\containers.csv") do (
  docker network connect YADAMU-NET %%a
)

