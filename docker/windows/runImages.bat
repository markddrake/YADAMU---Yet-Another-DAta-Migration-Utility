docker run --name ORA2103-01 -p1525:1521 --memory 8g -d yadamu/oracle:21
docker run --name ORA1903-01 -p1521:1521 --memory 8g -d yadamu/oracle:19
docker run --name ORA1803-01 -p1522:1521 --memory 8g -d yadamu/oracle:18
docker run --name ORA1220-01 -p1523:1521 --memory 8g -d yadamu/oracle:12.2
docker run --name ORA1120-01 -p1524:1521 --memory 8g -d yadamu/oracle:11.2

docker run --name MSSQL14-01 -p1435:1433 --memory 8g -d -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" yadamu/mssql:2014
docker run --name MSSQL14-01 -p1435:1433 --memory 8g -d -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" yadamu/svr2016/mssql:2014
docker run --name MSSQL17-01 -p1433:1433 --memory 8g -d -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" yadamu/mssql:2017
docker run --name MSSQL17-01 -p1433:1433 --memory 8g -d -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" yadamu/svr2016/mssql:2017-CU26
docker run --name MSSQL19-01 -p1434:1433 --memory 8g -d -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" yadamu/mssql:2019
docker run --name MSSQL19-01 -p1434:1433 --memory 8g -d -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=oracle#1" yadamu/mssql:2019-CU13

docker run --name MYSQL80-01 -p3306:3306 --memory 8g -d yadamu/mysql:8

docker run --name MARIA10-01 -p3307:3306 --memory 8g -d yadamu/mariadb:10

docker run --name MONGO05-01 -p27017:27017 --memory 8g -d yadamu/mongodb:

docker run --name PGSQL14-01 -p5432:5432 --memory 8g -d yadamu/postgres:14