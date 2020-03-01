docker load < /c/Docker/Images/Oracle_EE_19.3.0.0.0.tar
docker load < /c/Docker/Images/Oracle_EE_18.3.0.0.0.tar
docker load < /c/Docker/Images/Oracle_EE_12.2.0.1.0.tar
docker load < /c/Docker/Images/Oracle_EE_12.1.0.2.0.tar
docker load < /c/Docker/Images/Oracle_EE_11.2.0.1.0.tar
docker pull mysql
docker pull postgres
docker pull mariadb
docker pull mcr.microsoft.com/mssql/server:2017-latest
docker pull mcr.microsoft.com/mssql/server:2019-latest
docker volume create ORA1903-01
docker volume create ORA1803-01
docker volume create ORA1220-01
docker volume create ORA1210-01
docker volume create ORA1120-01
docker volume create MYSQL80-01
docker volume create MSSQL17-01
docker volume create MSSQL19-01
docker volume create PGSQL12-01
docker volume create MARIA10-01
# Oracle 19.3.0.0.0
docker run --name ORA1903-01  --shm-size="8g" -p 1521:1521 -e ORACLE_SID=CDB19300 -e ORACLE_PDB=PDB19300 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET='AL32UTF8' -vORA1903-01:/opt/oracle/oradata -d oracle/database:19.3.0-ee
# Oracle 18.3.0.0.0
docker run --name ORA1803-01  --shm-size="8g" -p 1522:1521 -e ORACLE_SID=CDB18300 -e ORACLE_PDB=PDB18300 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET='AL32UTF8' -vORA1803-01:/opt/oracle/oradata -d oracle/database:18.3.0-ee
# Oracle 12.2.0.1.0
docker run --name ORA1220-01  --shm-size="8g" -p 1523:1521 -e ORACLE_SID=CDB12200 -e ORACLE_PDB=PDB12200 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET='AL32UTF8' -vORA1220-01:/opt/oracle/oradata -d oracle/database:12.2.0.1-ee 
# Oracle 12.1.0.2.0
docker run --name ORA1210-01  --shm-size="8g" -p 1524:1521 -e ORACLE_SID=CDB12100 -e ORACLE_PDB=PDB12100 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET='AL32UTF8' -vORA1210-01:/opt/oracle/oradata -d oracle/database:12.1.0.2-ee 
# Oracle 11.2.0.1.0
docker run --name ORA1120-01  --shm-size="8g" -p 1525:1521 -e ORACLE_SID=ORA11200 -e ORACLE_PDB=ORA11200 -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET='AL32UTF8' -vORA1120-01:/opt/oracle/oradata -d oracle/database:11.2.0.1-ee
# Oracle MySQL 8.0
docker run --name MYSQL80-01  --shm-size="8g" -p 3306:3306  -v MYSQL80-01:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=oracle -d mysql:latest 
# MaraDB 10.0
docker run --name MARIA10-01  --shm-size="8g" -p 3307:3306 -v MARIA10-01:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=oracle -d mariadb:latest  
# MsSQL 2017
docker run --name MSSQL17-01  --shm-size="8g" -p 1433:1433 -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" -v MSSQL17-01:/var/opt/mssql  -d mcr.microsoft.com/mssql/server:2017-latest
# MsSQL 2019
docker run --name MSSQL19-01  --shm-size="8g" -p 1434:1433 -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" -v MSSQL19-01:/var/opt/mssql  -d mcr.microsoft.com/mssql/server:2019-latest
# Postgres 12
docker run --name PGSQL12-01  --shm-size="8g" -p 5432:5432 -e POSTGRES_PASSWORD=oracle -v PGSQL12-01:/var/lib/postgresql/data -d postgres:latest 