docker build -t yadamu/svr-2022:vs22             --file=..\Windows\dockerfile.2022.vs22         ..\Windows
docker build -t yadamu/svr-2022:vs13             --file=..\Windows\dockerfile.2022.vs13         ..\Windows
docker build -t yadamu/svr-2019:vs22             --file=..\Windows\dockerfile.2019.vs22         ..\Windows
docker build -t yadamu/svr-2019:vs13             --file=..\Windows\dockerfile.2019.vs13         ..\Windows
docker build -t yadamu/svr-2016:vs22             --file=..\Windows\dockerfile.2016.vs22         ..\Windows
docker tag yadamu/svr-2022:vs22   yadamu/svr:vs22   
docker tag yadamu/svr-2022:vs13   yadamu/svr:vs13   

docker build -t yadamu/mssql:2022                --file=..\MsSQL\Windows\2022\dockerfile        ..\MsSQL\Windows\2022
docker build -t yadamu/mssql:2019                --file=..\MsSQL\Windows\2019\dockerfile        ..\MsSQL\Windows\2019
docker build -t yadamu/mssql:2019-CU13           --file=..\MsSQL\Windows\2019\dockerfile.CU13   ..\MsSQL\Windows\2019
docker build -t yadamu/mssql:2017                --file=..\MsSQL\Windows\2017\dockerfile        ..\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2017       --file=..\MsSQL\Windows\2017\dockerfile.2016   ..\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2017-CU26  --file=..\MsSQL\Windows\2017\dockerfile.CU26   ..\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2014       --file=..\MsSQL\Windows\2014\dockerfile.2016   ..\MsSQL\Windows\2014

docker build -t yadamu/mysql:8.0                 --file=..\mysql\windows\8.0\dockerfile          ..\mysql\windows\8.0
docker build -t yadamu/mysql:8.1                 --file=..\mysql\windows\8.1\dockerfile          ..\mysql\windows\8.1
docker build -t yadamu/mariadb:10                --file=..\mariadb\windows\10\dockerfile        ..\mariadb\windows\10
docker build -t yadamu/mariadb:11                --file=..\mariadb\windows\11\dockerfile        ..\mariadb\windows\11
docker build -t yadamu/mongodb:5                 --file=..\mongodb\windows\05\dockerfile        ..\mongodb\windows\05
docker build -t yadamu/mongodb:7                 --file=..\mongodb\windows\07\dockerfile        ..\mongodb\windows\07
docker build -t yadamu/postgres:14               --file=..\postgres\windows\14\dockerfile       ..\postgres\windows\14
docker build -t yadamu/postgres:16               --file=..\postgres\windows\16\dockerfile       ..\postgres\windows\16

docker build -t yadamu/oracle:21.3.0             --file=..\oracle\Windows\21.3\dockerfile       ..\oracle\Windows\21.3
docker build -t yadamu/oracle:19.3.0             --file=..\oracle\Windows\19.3\dockerfile       ..\oracle\Windows\19.3
docker build -t yadamu/oracle:18.3.0             --file=..\oracle\Windows\18.3\dockerfile       ..\oracle\Windows\18.3
docker build -t yadamu/oracle:12.2.0             --file=..\oracle\Windows\12.2\dockerfile       ..\oracle\Windows\12.2
docker build -t yadamu/oracle:11.2.0.4           --file=..\oracle\Windows\11.2.0.4\dockerfile   ..\oracle\Windows\11.2.0.4
docker build -t yadamu/oracle:11.2.0.1           --file=..\oracle\Windows\11.2.0.1\dockerfile   ..\oracle\Windows\11.2.0.1
						               								           
